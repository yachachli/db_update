"""SportMonks API client for the World Cup 2026 predictive model.

Thin wrapper around the SportMonks REST API. This iteration covers only the
plumbing: query-parameter authentication, a single ``get`` method, on-disk
response caching under ``data/cache/``, and logging. It intentionally does
**no** fixture parsing or business-logic transformation -- callers receive
the raw parsed JSON dict so the response structure can be inspected first.
"""

from __future__ import annotations

import hashlib
import json
import logging
import urllib.parse
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

from src.config import SPORTMONKS_API_KEY, SPORTMONKS_BASE_URL

__all__ = [
    "SportmonksClient",
    "SportmonksError",
    "QUALIFIER_LEAGUE_IDS",
    "WC_FINALS_LEAGUE_ID",
]

logger = logging.getLogger(__name__)

# On-disk cache location, relative to the project root (this file is at
# <root>/src/sportmonks_client.py).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_DIR = _PROJECT_ROOT / "data" / "cache"

_REQUEST_TIMEOUT_SECONDS = 30
_API_TOKEN_PARAM = "api_token"

# All World-Cup-relevant league IDs discovered on our plan (see
# data/leagues_catalog.json). These are stable identifiers and safe to
# hardcode. The full universe of our "World Cup 2026" plan is just these 8.
WC_FINALS_LEAGUE_ID = 732  # "World Cup" (the finals)
QUALIFIER_LEAGUE_IDS: list[int] = [
    732,  # World Cup (finals)
    720,  # WC Qualification Europe
    714,  # WC Qualification Asia
    717,  # WC Qualification Concacaf
    723,  # WC Qualification Oceania
    726,  # WC Qualification South America
    711,  # CAF World Cup Qualifiers
    729,  # WC Qualification Intercontinental Playoffs
]

# The "xGFixture" include is misleadingly named: it does NOT return only xG.
# Diagnostic work (scripts/resolve_xg_types.py) confirmed it returns the FULL
# per-team statistics collection (54 stat types: goals, shots, possession,
# big chances, AND the expected-goals family), keyed by participant_id. It is
# the single include we need to populate every MatchStats field.
_FIXTURE_INCLUDE = "xGFixture;participants;scores;state;venue"

# How far back to look when fetching a team's recent fixtures. Covers the full
# 2026 World Cup qualifying cycle. We fetch a generous page within the window
# and sort most-recent-first client-side (the API returns them ascending).
_FIXTURE_LOOKBACK_DAYS = 365 * 4
_FIXTURE_FETCH_PAGE = 100


class SportmonksError(RuntimeError):
    """Raised for configuration problems or non-2xx SportMonks responses."""


class SportmonksClient:
    """Minimal authenticated client for the SportMonks football API.

    Authentication is via the ``api_token`` query parameter, appended to every
    request. Responses are cached on disk (keyed by the request URL *excluding*
    the token) so repeated calls during development do not burn API quota.
    """

    def __init__(
        self,
        api_key: str | None = SPORTMONKS_API_KEY,
        base_url: str = SPORTMONKS_BASE_URL,
        cache_dir: Path = _CACHE_DIR,
    ) -> None:
        if not api_key:
            raise SportmonksError(
                "SPORTMONKS_API_KEY is not set. Add it to your .env file "
                "(see .env.example) or pass api_key explicitly."
            )
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("SportmonksClient initialized (base_url=%s)", self._base_url)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Perform an authenticated GET request, with on-disk caching.

        Args:
            endpoint: Path appended to the base URL (e.g. ``"leagues"``).
            params: Query parameters (excluding ``api_token``, which is added
                automatically).
            force_refresh: If True, ignore any cached response and re-fetch.

        Returns:
            The parsed JSON response as a dict.

        Raises:
            SportmonksError: on a non-2xx response (message includes the
                status code and response body) or a network failure.
        """
        params = dict(params or {})
        url = f"{self._base_url}/{endpoint.lstrip('/')}"

        cache_key = self._cache_key(url, params)
        cache_path = self._cache_dir / f"{cache_key}.json"

        if not force_refresh and cache_path.exists():
            logger.info("Cache HIT for %s (%s)", endpoint, cache_path.name)
            return json.loads(cache_path.read_text(encoding="utf-8"))

        logger.info(
            "Cache %s for %s -> requesting %s",
            "BYPASS" if force_refresh else "MISS",
            endpoint,
            self._redact(url, params),
        )

        request_params = {**params, _API_TOKEN_PARAM: self._api_key}
        try:
            response = requests.get(
                url, params=request_params, timeout=_REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as exc:
            raise SportmonksError(
                f"Network error requesting {self._redact(url, params)}: {exc}"
            ) from exc

        logger.info(
            "Response %s for %s (%d bytes)",
            response.status_code,
            endpoint,
            len(response.content),
        )

        if not response.ok:
            raise SportmonksError(
                f"SportMonks request to {self._redact(url, params)} failed "
                f"with HTTP {response.status_code}: {response.text}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise SportmonksError(
                f"SportMonks response for {endpoint} was not valid JSON: {exc}"
            ) from exc

        cache_path.write_text(
            json.dumps(payload, indent=2), encoding="utf-8"
        )
        logger.debug("Cached response at %s", cache_path)
        return payload

    def clear_cache(self) -> int:
        """Delete every cached response file. Returns the number removed."""
        removed = 0
        for cache_file in self._cache_dir.glob("*.json"):
            cache_file.unlink()
            removed += 1
        logger.info("Cleared %d cached response(s) from %s", removed, self._cache_dir)
        return removed

    # ------------------------------------------------------------------
    # Fixture fetching
    # ------------------------------------------------------------------
    def get_fixtures_for_team(
        self,
        team_id: int,
        limit: int = 25,
        season_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a team's most recent fixtures, scoped to our competitions.

        Uses the date-ranged ``/fixtures/between/{start}/{end}/{team_id}``
        endpoint, which is the v3 way to retrieve fixtures *for a specific
        team* (a plain ``/fixtures`` league filter does NOT restrict to a
        team -- it returns the whole league). Results are scoped to our
        WC-relevant leagues (and optionally a single season), sorted
        most-recent-first, and truncated to ``limit``.

        Uses the shared cached ``get`` so re-running on the same day hits the
        cache. Returns raw fixture dicts, unwrapped from the ``data`` envelope.
        """
        end = date.today()
        start = end - timedelta(days=_FIXTURE_LOOKBACK_DAYS)

        league_csv = ",".join(str(lid) for lid in QUALIFIER_LEAGUE_IDS)
        filter_parts = [f"fixtureLeagues:{league_csv}"]
        if season_id is not None:
            filter_parts.append(f"fixtureSeasons:{season_id}")

        response = self.get(
            f"fixtures/between/{start.isoformat()}/{end.isoformat()}/{team_id}",
            params={
                "filters": ";".join(filter_parts),
                "include": _FIXTURE_INCLUDE,
                "per_page": _FIXTURE_FETCH_PAGE,
            },
        )
        data = response.get("data")
        if not isinstance(data, list):
            return []

        # The endpoint returns fixtures ascending; we want the most recent.
        data.sort(key=lambda f: f.get("starting_at_timestamp") or 0, reverse=True)
        return data[:limit]

    def get_fixture_by_id(self, fixture_id: int) -> dict[str, Any]:
        """Fetch a single fixture with full per-team stats included.

        Returns the raw fixture dict (unwrapped from the ``data`` envelope).
        """
        response = self.get(
            f"fixtures/{fixture_id}",
            params={"include": _FIXTURE_INCLUDE},
        )
        data = response.get("data")
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _cache_key(url: str, params: dict[str, Any]) -> str:
        """SHA256 of the full URL + params, excluding the api_token.

        Excluding the token means rotating API keys does not invalidate the
        cache. Params are sorted so key order is irrelevant.
        """
        cacheable = {k: v for k, v in params.items() if k != _API_TOKEN_PARAM}
        query = urllib.parse.urlencode(sorted(cacheable.items()))
        canonical = f"{url}?{query}" if query else url
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _redact(url: str, params: dict[str, Any]) -> str:
        """Render a URL+params string for logs with the api_token masked."""
        shown = {k: v for k, v in params.items() if k != _API_TOKEN_PARAM}
        shown[_API_TOKEN_PARAM] = "***REDACTED***"
        query = urllib.parse.urlencode(shown)
        return f"{url}?{query}"
