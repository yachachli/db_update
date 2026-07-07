"""MLB Stats API client — minimal subset for mlb_results_update.

Free, public, no auth required. Source: statsapi.mlb.com.
"""
from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

import requests

logger = logging.getLogger("mlb_results_update.mlb_stats_client")

BASE_URL = "https://statsapi.mlb.com/api"
SPORT_ID_MLB = 1
HTTP_TIMEOUT_SEC = 15.0
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE_SEC = 1.0
RETRY_STATUSES = (429, 500, 502, 503, 504)


class HttpError(RuntimeError):
    pass


def _politeness_sleep() -> None:
    """Optional per-request pause, enabled only during long backfills via
    MLB_REQUEST_SLEEP_SEC (set by __main__ in backfill mode). No-op otherwise."""
    try:
        secs = float(os.getenv("MLB_REQUEST_SLEEP_SEC", "0") or 0)
    except ValueError:
        secs = 0.0
    if secs > 0:
        time.sleep(secs)


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
        _politeness_sleep()
        return response.json()

    raise HttpError(f"Exhausted retries for {url}: {last_exc}")


class MLBStatsClient:
    """No auth required. Only the endpoints this folder needs."""

    def get_schedule(self, target_date: str) -> list[dict[str, Any]]:
        """All games on target_date (YYYY-MM-DD). Includes status + scores when Final."""
        payload = _http_get(
            f"{BASE_URL}/v1/schedule",
            params={"sportId": SPORT_ID_MLB, "date": target_date},
        )
        games: list[dict[str, Any]] = []
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                games.append(game)
        return games

    def get_box_score(self, game_pk: int) -> dict[str, Any]:
        """Full box score with per-player batting + pitching stats."""
        payload = _http_get(f"{BASE_URL}/v1/game/{game_pk}/boxscore")
        if not isinstance(payload, dict) or "teams" not in payload:
            raise HttpError(f"Unexpected boxscore shape for game {game_pk}")
        return payload

    def get_line_score(self, game_pk: int) -> dict[str, Any]:
        """Compact line score — used for extra_innings detection."""
        payload = _http_get(f"{BASE_URL}/v1/game/{game_pk}/linescore")
        if not isinstance(payload, dict):
            raise HttpError(f"Unexpected linescore shape for game {game_pk}")
        return payload

    def get_person(self, person_id: int) -> dict[str, Any]:
        """Player metadata: name, throws, bats, position, debut, birth."""
        payload = _http_get(f"{BASE_URL}/v1/people/{person_id}")
        people = payload.get("people", [])
        if not people:
            raise HttpError(f"Empty /people response for {person_id}")
        return people[0]
