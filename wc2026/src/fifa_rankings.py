"""FIFA men's world ranking client for the World Cup 2026 predictive model.

FIFA does not publish a documented rankings API, but the public ranking page
(https://inside.fifa.com/fifa-world-ranking/men) is backed by an undocumented
JSON endpoint:

    GET https://inside.fifa.com/api/ranking-overview?locale=en&dateId=id{N}

Investigation (scripts/probe_latest_fifa_dateid.py) established three facts we
rely on here:

1. ``dateId`` is a **daily counter**: ``id14870`` == 2025-09-18, and each
   calendar day increments the number by one. The relation is linear, anchored
   on the verified pair (REFERENCE_DATE_ID, REFERENCE_DATE).
2. Only days on which FIFA actually published a release (~12/year) return data.
   Every other ``dateId`` returns a ~15-byte empty body (HTTP 200).
3. Calling the endpoint with **no** ``dateId`` returns an empty body, the page's
   embedded ``__NEXT_DATA__`` date list is stale, and there is no index/calendar
   endpoint. So the only reliable way to find the latest release is to compute
   today's theoretical ``dateId`` and walk backward until data appears.

This module owns that discovery, defensive parsing into immutable dataclasses,
and on-disk caching (resolved-dateId with a 7-day TTL; release payloads, and
empty-day markers, cached indefinitely since published history never changes).
"""

from __future__ import annotations

import json
import logging
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

__all__ = [
    "FifaRankingEntry",
    "FifaRankingRelease",
    "FifaRankingsClient",
    "FifaRankingsError",
    "get_current_rankings",
    "get_ranking_for_team",
]

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_DIR = _PROJECT_ROOT / "data" / "cache" / "fifa"


class FifaRankingsError(RuntimeError):
    """Raised on network/HTTP failures or when no release can be discovered."""


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class FifaRankingEntry:
    """A single team's row in a FIFA ranking release."""

    rank: int
    name: str
    country_code: str
    points: float
    previous_rank: int | None = None
    previous_points: float | None = None
    confederation: str | None = None
    team_id: int | None = None


@dataclass(frozen=True, slots=True)
class FifaRankingRelease:
    """A full FIFA ranking release for one publication date.

    ``entries`` is sorted by rank ascending. Lookups are case-insensitive.
    """

    date_id: str
    release_date: str  # ISO date, e.g. "2026-04-01"
    entries: tuple[FifaRankingEntry, ...]

    def __len__(self) -> int:
        return len(self.entries)

    def top(self, n: int = 10) -> tuple[FifaRankingEntry, ...]:
        """Return the top ``n`` teams (already rank-sorted)."""
        return self.entries[:n]

    def lookup_by_code(self, country_code: str) -> FifaRankingEntry | None:
        """Find a team by its three-letter country code (e.g. ``"FRA"``)."""
        target = country_code.strip().upper()
        for entry in self.entries:
            if entry.country_code.upper() == target:
                return entry
        return None

    def lookup_by_name(self, name: str) -> FifaRankingEntry | None:
        """Find a team by exact (case-insensitive) name (e.g. ``"France"``)."""
        target = name.strip().casefold()
        for entry in self.entries:
            if entry.name.casefold() == target:
                return entry
        return None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class FifaRankingsClient:
    """Discovers, fetches, parses and caches FIFA world ranking releases."""

    API_URL = "https://inside.fifa.com/api/ranking-overview"
    PAGE_URL = "https://inside.fifa.com/fifa-world-ranking/men"

    # Verified anchor: id14870 == 2025-09-18 (see module docstring).
    REFERENCE_DATE_ID = 14870
    REFERENCE_DATE = date(2025, 9, 18)

    # Releases are at most ~2 months apart; 90 days is a generous safety cap
    # that bounds the walk without risking an unbounded loop on an outage.
    MAX_WALKBACK_DAYS = 90

    # Off-release dateIds return a ~15-byte empty body; anything smaller than
    # this is treated as "no release on that day".
    EMPTY_RESPONSE_THRESHOLD = 50

    # Politeness delay between live API calls during the walk-back. 0.3s is
    # plenty (0.5s was overkill in test runs); cached lookups incur no delay.
    WALKBACK_DELAY_SECONDS = 0.3

    # Re-resolve the latest dateId weekly even if a cached value exists.
    LATEST_DATEID_TTL_DAYS = 7

    HTTP_TIMEOUT_SECONDS = 30

    BROWSER_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": PAGE_URL,
    }

    def __init__(self, cache_dir: Path = _CACHE_DIR) -> None:
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._api_call_count = 0
        logger.debug("FifaRankingsClient initialized (cache=%s)", self._cache_dir)

    # -- introspection -------------------------------------------------------
    @property
    def api_call_count(self) -> int:
        """Number of live HTTP requests made (cache hits do not count)."""
        return self._api_call_count

    # -- dateId math ---------------------------------------------------------
    def date_id_for(self, day: date) -> str:
        """Map a calendar date to its FIFA ``dateId`` via the daily counter."""
        return f"id{self.REFERENCE_DATE_ID + (day - self.REFERENCE_DATE).days}"

    # -- discovery -----------------------------------------------------------
    def discover_latest_date_id(self, force_refresh: bool = False) -> str:
        """Resolve the dateId of the most recent FIFA ranking release.

        FIFA's dateId is a daily counter: id14870 = 2025-09-18, and each
        subsequent day adds 1. Only days with a release contain data (~12 per
        year); other dateIds return ~15-byte empty bodies.

        Strategy: compute today's theoretical dateId, then walk backward one
        day at a time hitting the API until we get a non-empty response. Cap at
        MAX_WALKBACK_DAYS to handle unusual release gaps without infinite
        walking.

        Result is cached to data/cache/fifa/latest_dateid.json with a 7-day
        TTL. Pass ``force_refresh=True`` to bypass.
        """
        cached = self._read_latest_cache()
        if not force_refresh and cached is not None:
            logger.info(
                "latest dateId cache HIT: %s (release %s, resolved %s) -- 0 API calls",
                cached["date_id"],
                cached.get("release_date"),
                cached.get("resolved_at"),
            )
            return cached["date_id"]

        today = date.today()
        logger.info(
            "Walking back from %s (%s) to discover the latest release...",
            today.isoformat(),
            self.date_id_for(today),
        )

        for offset in range(self.MAX_WALKBACK_DAYS + 1):
            day = today - timedelta(days=offset)
            date_id = self.date_id_for(day)
            payload, was_network = self._get_payload(date_id)

            if offset and offset % 10 == 0:
                logger.info("  ...checked %d days (currently at %s / %s)",
                            offset, day.isoformat(), date_id)

            if payload is not None:
                release_date = _release_date_from_payload(payload) or day.isoformat()
                self._write_latest_cache(date_id, release_date)
                logger.info(
                    "Discovered latest release: %s -> %s (%d day(s) back, %d API call(s))",
                    release_date, date_id, offset, self._api_call_count,
                )
                return date_id

            if was_network:
                time.sleep(self.WALKBACK_DELAY_SECONDS)

        raise FifaRankingsError(
            f"No FIFA ranking release found within {self.MAX_WALKBACK_DAYS} days "
            f"before {today.isoformat()} (checked back to "
            f"{(today - timedelta(days=self.MAX_WALKBACK_DAYS)).isoformat()}). "
            "The dateId anchor may have drifted or the API may be down."
        )

    # -- fetching ------------------------------------------------------------
    def fetch_ranking(
        self, date_id: str, force_refresh: bool = False
    ) -> FifaRankingRelease:
        """Fetch and parse the ranking release for a specific ``dateId``."""
        payload, _ = self._get_payload(date_id, force_refresh=force_refresh)
        if payload is None:
            raise FifaRankingsError(
                f"No FIFA ranking release exists for {date_id} "
                "(the API returned an empty body for that day)."
            )
        return _parse_release(date_id, payload)

    def fetch_latest(self, force_refresh: bool = False) -> FifaRankingRelease:
        """Discover the latest dateId and fetch its release."""
        date_id = self.discover_latest_date_id(force_refresh=force_refresh)
        return self.fetch_ranking(date_id)

    def fetch_all_known_rankings(self) -> list[FifaRankingRelease]:
        """Fetch every release whose dateId is embedded in the FIFA page.

        The page's ``__NEXT_DATA__`` lists historical release dateIds. Each is
        fetched via :meth:`fetch_ranking` (cached indefinitely), so the first
        call is slow but subsequent calls are served from disk. Malformed or
        empty dateIds are skipped with a warning.
        """
        releases: list[FifaRankingRelease] = []
        for date_id in self._known_date_ids_from_page():
            try:
                releases.append(self.fetch_ranking(date_id))
            except FifaRankingsError as exc:
                warnings.warn(f"Skipping {date_id}: {exc}")
        releases.sort(key=lambda r: r.release_date)
        return releases

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _get_payload(
        self, date_id: str, force_refresh: bool = False
    ) -> tuple[dict[str, Any] | None, bool]:
        """Return (release_payload_or_None, was_network_call).

        ``None`` means the day had no release. Release payloads and empty-day
        markers are both cached: empty markers stop the walk-back from
        re-probing slots we have already verified are empty.
        """
        cache_path = self._ranking_cache_path(date_id)
        if not force_refresh and cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("_empty"):
                return None, False
            return cached, False

        size, payload = self._http_get(date_id)
        is_empty = (
            payload is None
            or size < self.EMPTY_RESPONSE_THRESHOLD
            or not payload.get("rankings")
        )
        if is_empty:
            cache_path.write_text(
                json.dumps({"_empty": True, "size": size, "date_id": date_id}),
                encoding="utf-8",
            )
            return None, True

        cache_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        return payload, True

    def _http_get(self, date_id: str) -> tuple[int, dict[str, Any] | None]:
        """Perform the live GET. Returns (body_size, parsed_json_or_None)."""
        self._api_call_count += 1
        try:
            resp = requests.get(
                self.API_URL,
                headers=self.BROWSER_HEADERS,
                params={"locale": "en", "dateId": date_id},
                timeout=self.HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise FifaRankingsError(
                f"Network error fetching FIFA ranking {date_id}: {exc}"
            ) from exc

        if not resp.ok:
            raise FifaRankingsError(
                f"FIFA ranking request for {date_id} failed with HTTP "
                f"{resp.status_code}: {resp.text[:200]}"
            )

        size = len(resp.text)
        try:
            return size, resp.json()
        except ValueError:
            return size, None

    def _known_date_ids_from_page(self) -> list[str]:
        """Extract historical release dateIds from the page's __NEXT_DATA__."""
        try:
            resp = requests.get(
                self.PAGE_URL,
                headers={**self.BROWSER_HEADERS, "Accept": "text/html,*/*"},
                timeout=self.HTTP_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise FifaRankingsError(f"Failed to fetch FIFA ranking page: {exc}") from exc

        import re

        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text,
            re.DOTALL,
        )
        if not match:
            raise FifaRankingsError("Could not locate __NEXT_DATA__ on the FIFA page.")

        data = json.loads(match.group(1))
        date_ids: set[str] = set()

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                nid = node.get("id")
                if isinstance(nid, str) and nid.startswith("id") and nid[2:].isdigit():
                    date_ids.add(nid)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(data)
        return sorted(date_ids, key=lambda i: int(i[2:]))

    # -- latest-dateId cache -------------------------------------------------
    @property
    def _latest_cache_path(self) -> Path:
        return self._cache_dir / "latest_dateid.json"

    def _read_latest_cache(self) -> dict[str, Any] | None:
        path = self._latest_cache_path
        if not path.exists():
            return None
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            resolved_at = datetime.fromisoformat(cached["resolved_at"])
        except (ValueError, KeyError, OSError):
            return None
        if datetime.now() - resolved_at > timedelta(days=self.LATEST_DATEID_TTL_DAYS):
            logger.info("latest dateId cache STALE (resolved %s); will re-walk.",
                        cached.get("resolved_at"))
            return None
        return cached

    def _write_latest_cache(self, date_id: str, release_date: str) -> None:
        self._latest_cache_path.write_text(
            json.dumps(
                {
                    "date_id": date_id,
                    "release_date": release_date,
                    "resolved_at": datetime.now().isoformat(timespec="seconds"),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    def _ranking_cache_path(self, date_id: str) -> Path:
        return self._cache_dir / f"ranking_{date_id}.json"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _release_date_from_payload(payload: dict[str, Any]) -> str | None:
    """Pull the ISO release date (YYYY-MM-DD) from the first entry."""
    for entry in payload.get("rankings") or []:
        last_update = entry.get("lastUpdateDate")
        if isinstance(last_update, str) and len(last_update) >= 10:
            return last_update[:10]
    return None


def _parse_release(date_id: str, payload: dict[str, Any]) -> FifaRankingRelease:
    """Defensively parse a raw API payload into a FifaRankingRelease.

    Malformed rows are skipped with a warning rather than failing the release.
    """
    raw_entries = payload.get("rankings") or []
    entries: list[FifaRankingEntry] = []

    for raw in raw_entries:
        try:
            item = raw["rankingItem"]
            tag = raw.get("tag") or {}
            entries.append(
                FifaRankingEntry(
                    rank=int(item["rank"]),
                    name=str(item["name"]),
                    country_code=str(item.get("countryCode", "")),
                    points=float(item["totalPoints"]),
                    previous_rank=_safe_int(item.get("previousRank")),
                    previous_points=_safe_float(raw.get("previousPoints")),
                    confederation=tag.get("text"),
                    team_id=_safe_int(item.get("idTeam")),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            warnings.warn(f"Skipping malformed FIFA ranking entry in {date_id}: {exc}")

    entries.sort(key=lambda e: e.rank)
    release_date = _release_date_from_payload(payload) or ""
    return FifaRankingRelease(
        date_id=date_id, release_date=release_date, entries=tuple(entries)
    )


# ---------------------------------------------------------------------------
# Module-level convenience helpers
# ---------------------------------------------------------------------------
def get_current_rankings(force_refresh: bool = False) -> FifaRankingRelease:
    """Return the most recent FIFA ranking release."""
    return FifaRankingsClient().fetch_latest(force_refresh=force_refresh)


def get_ranking_for_team(team: str) -> FifaRankingEntry | None:
    """Look up a single team in the current ranking by code or name.

    A three-letter all-caps string is treated as a country code; otherwise the
    value is matched against team names. Returns ``None`` if not found.
    """
    release = get_current_rankings()
    if len(team) == 3 and team.isupper():
        entry = release.lookup_by_code(team)
        if entry is not None:
            return entry
    return release.lookup_by_code(team) or release.lookup_by_name(team)
