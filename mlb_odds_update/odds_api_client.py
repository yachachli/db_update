"""The Odds API client — minimal subset for mlb_odds_update.

Source: https://the-odds-api.com/  (v4). Auth via the `apiKey` query param.
Mirrors the retry/backoff style of the sibling MLBStatsClient folders.

Credit cost per request = markets x regions. The response headers
`x-requests-remaining` / `x-requests-used` are captured on every call so the
pipeline can report usage at the end of a run.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

logger = logging.getLogger("mlb_odds_update.odds_api_client")

BASE_URL = "https://api.the-odds-api.com/v4"
SPORT_KEY = "baseball_mlb"
HTTP_TIMEOUT_SEC = 20.0
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE_SEC = 1.0
RETRY_STATUSES = (429, 500, 502, 503, 504)

# Full-game featured markets (bulk endpoint).
FEATURED_MARKETS = ("h2h", "spreads", "totals")
# First-5-innings markets (per-event endpoint).
F5_MARKETS = ("h2h_1st_5_innings", "spreads_1st_5_innings", "totals_1st_5_innings")


class OddsApiError(RuntimeError):
    pass


class OddsAPIClient:
    """Thin client for the two endpoints this folder needs. No state beyond the
    latest credit-usage headers, which the pipeline logs at end of run."""

    def __init__(self, api_key: str, regions: str = "us", odds_format: str = "american") -> None:
        if not api_key:
            raise OddsApiError("ODDS_API_KEY is not set — check env / GitHub Secrets.")
        self.api_key = api_key
        self.regions = regions
        self.odds_format = odds_format
        self.requests_remaining: str | None = None
        self.requests_used: str | None = None

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        params = {**params, "apiKey": self.api_key}
        url = f"{BASE_URL}{path}"
        last_exc: Exception | None = None
        for attempt in range(1, HTTP_MAX_RETRIES + 1):
            try:
                response = requests.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
            except requests.RequestException as e:
                last_exc = e
                if attempt < HTTP_MAX_RETRIES:
                    wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    logger.warning("GET %s failed (%s) — retry %d/%d in %.1fs",
                                   path, e, attempt, HTTP_MAX_RETRIES, wait)
                    time.sleep(wait)
                    continue
                raise OddsApiError(f"GET {path} failed after {HTTP_MAX_RETRIES} attempts: {e}") from e

            # Track credit usage from every response (headers present even on errors).
            if "x-requests-remaining" in response.headers:
                self.requests_remaining = response.headers.get("x-requests-remaining")
            if "x-requests-used" in response.headers:
                self.requests_used = response.headers.get("x-requests-used")

            if response.status_code in RETRY_STATUSES and attempt < HTTP_MAX_RETRIES:
                wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning("GET %s returned %d — retry %d/%d in %.1fs",
                               path, response.status_code, attempt, HTTP_MAX_RETRIES, wait)
                time.sleep(wait)
                continue

            if not response.ok:
                raise OddsApiError(f"HTTP {response.status_code} for GET {path}: {response.text[:300]}")
            return response.json()

        raise OddsApiError(f"Exhausted retries for {path}: {last_exc}")

    def get_featured_odds(self) -> list[dict[str, Any]]:
        """Bulk endpoint — one call covers the whole slate's full-game markets."""
        payload = self._get(
            f"/sports/{SPORT_KEY}/odds",
            {
                "regions": self.regions,
                "markets": ",".join(FEATURED_MARKETS),
                "oddsFormat": self.odds_format,
            },
        )
        if not isinstance(payload, list):
            raise OddsApiError(f"Unexpected featured-odds shape: {type(payload).__name__}")
        return payload

    def get_event_f5_odds(self, event_id: str) -> dict[str, Any]:
        """Per-event endpoint for first-5-innings markets. Returns a single event dict
        (with a possibly-empty bookmakers list if no book posts F5 lines yet)."""
        payload = self._get(
            f"/sports/{SPORT_KEY}/events/{event_id}/odds",
            {
                "regions": self.regions,
                "markets": ",".join(F5_MARKETS),
                "oddsFormat": self.odds_format,
            },
        )
        if not isinstance(payload, dict):
            raise OddsApiError(f"Unexpected event-odds shape for {event_id}: {type(payload).__name__}")
        return payload

    def get_historical_odds(self, date_iso: str) -> dict[str, Any]:
        """Bulk historical snapshot for full-game markets on a given timestamp.

        date_iso must be ISO8601 (e.g. 2024-06-15T21:00:00Z). YYYY-MM-DD is expanded
        to noon UTC by the caller. Returns {timestamp, data: [events...], ...}.
        """
        payload = self._get(
            f"/historical/sports/{SPORT_KEY}/odds",
            {
                "regions": self.regions,
                "markets": ",".join(FEATURED_MARKETS),
                "oddsFormat": self.odds_format,
                "date": date_iso,
            },
        )
        if not isinstance(payload, dict):
            raise OddsApiError(
                f"Unexpected historical-odds shape: {type(payload).__name__}"
            )
        return payload

    def get_historical_event_odds(self, event_id: str, date_iso: str) -> dict[str, Any]:
        """Historical per-event odds (used for F5 period markets)."""
        payload = self._get(
            f"/historical/sports/{SPORT_KEY}/events/{event_id}/odds",
            {
                "regions": self.regions,
                "markets": ",".join(F5_MARKETS),
                "oddsFormat": self.odds_format,
                "date": date_iso,
            },
        )
        if not isinstance(payload, dict):
            raise OddsApiError(
                f"Unexpected historical event-odds shape for {event_id}: "
                f"{type(payload).__name__}"
            )
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(payload, dict) and payload.get("bookmakers") is not None:
            return payload
        raise OddsApiError(f"Historical event-odds for {event_id} missing data block")
