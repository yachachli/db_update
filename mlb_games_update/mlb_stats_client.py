"""MLB Stats API client — minimal subset for mlb_games_update.

Free, public, no auth required. Source: statsapi.mlb.com.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

logger = logging.getLogger("mlb_games_update.mlb_stats_client")

BASE_URL = "https://statsapi.mlb.com/api"
SPORT_ID_MLB = 1
HTTP_TIMEOUT_SEC = 15.0
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE_SEC = 1.0
RETRY_STATUSES = (429, 500, 502, 503, 504)


class HttpError(RuntimeError):
    pass


def _http_get(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=HTTP_TIMEOUT_SEC)
        except requests.RequestException as e:
            last_exc = e
            if attempt < HTTP_MAX_RETRIES:
                wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning("GET %s failed (%s) — retry %d/%d in %.1fs",
                               url, e, attempt, HTTP_MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            raise HttpError(f"GET {url} failed after {HTTP_MAX_RETRIES} attempts: {e}") from e

        if response.status_code in RETRY_STATUSES and attempt < HTTP_MAX_RETRIES:
            wait = HTTP_BACKOFF_BASE_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            logger.warning("GET %s returned %d — retry %d/%d in %.1fs",
                           url, response.status_code, attempt, HTTP_MAX_RETRIES, wait)
            time.sleep(wait)
            continue

        if not response.ok:
            raise HttpError(f"HTTP {response.status_code} for GET {url}: {response.text[:300]}")
        return response.json()

    raise HttpError(f"Exhausted retries for {url}: {last_exc}")


class MLBStatsClient:
    """Thin client. No auth, no rate limit handling needed beyond retries."""

    def get_teams(self, season: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"sportId": SPORT_ID_MLB}
        if season:
            params["season"] = str(season)
        payload = _http_get(f"{BASE_URL}/v1/teams", params=params)
        return payload.get("teams", [])

    def get_venue(self, venue_id: int) -> dict[str, Any]:
        payload = _http_get(f"{BASE_URL}/v1/venues/{venue_id}")
        venues = payload.get("venues", [])
        if not venues:
            raise HttpError(f"Empty venues for {venue_id}")
        return venues[0]

    def get_schedule_with_pitchers(self, target_date: str) -> list[dict[str, Any]]:
        """Date in YYYY-MM-DD format. Returns flat list of game dicts."""
        payload = _http_get(
            f"{BASE_URL}/v1/schedule",
            params={
                "sportId": SPORT_ID_MLB,
                "date": target_date,
                "hydrate": "probablePitcher,team,venue",
            },
        )
        games: list[dict[str, Any]] = []
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                games.append(game)
        return games
