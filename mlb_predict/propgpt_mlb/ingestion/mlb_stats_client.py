"""MLB Stats API client (statsapi.mlb.com).

The official, free, public MLB data API. We use it as the source of truth for:
- Game schedules and gamePks (canonical game_id)
- Final scores / outcomes
- Box scores (cross-check against Tank01)
- Probable pitchers
- Venue (park) metadata

No API key required. We're still polite — the shared request_with_retry handles
backoff on transient errors.

Date format: 'YYYY-MM-DD' (note the dashes — different from Tank01's YYYYMMDD).
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from .http import HttpError, request_with_retry

logger = logging.getLogger(__name__)

MLB_STATS_BASE_URL = "https://statsapi.mlb.com/api"
SPORT_ID_MLB = 1


class MLBStatsError(RuntimeError):
    """Raised when the MLB Stats API returns an unexpected shape."""


class MLBStatsClient:
    """Thin wrapper around statsapi.mlb.com.

    No auth. Constructor takes optional timeout override.
    """

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    # ---------- internals ----------

    def _get(self, path: str, params: dict[str, Any] | None = None, *, version: str = "v1") -> Any:
        url = f"{MLB_STATS_BASE_URL}/{version}{path}"
        logger.debug("MLBStats GET %s params=%s", path, params)
        response = request_with_retry(
            "GET",
            url,
            params=params,
            timeout=self.timeout,
        )
        try:
            return response.json()
        except ValueError as e:
            raise MLBStatsError(f"Non-JSON response from {path}: {e}") from e

    # ---------- schedule / games ----------

    def get_schedule(
        self,
        *,
        date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        season: int | str | None = None,
        team_id: int | None = None,
        hydrate: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Get MLB schedule. Returns a flat list of game dicts.

        Provide one of:
          - date='YYYY-MM-DD'                       → single day
          - start_date/end_date='YYYY-MM-DD'        → date range
          - season=YYYY                             → full season
        Optional: team_id to filter, hydrate for extra fields (e.g. ['probablePitcher','team','venue']).
        """
        params: dict[str, Any] = {"sportId": SPORT_ID_MLB}
        if date:
            self._validate_iso_date(date)
            params["date"] = date
        if start_date:
            self._validate_iso_date(start_date)
            params["startDate"] = start_date
        if end_date:
            self._validate_iso_date(end_date)
            params["endDate"] = end_date
        if season:
            params["season"] = str(season)
        if team_id:
            params["teamId"] = team_id
        if hydrate:
            params["hydrate"] = ",".join(hydrate)

        payload = self._get("/schedule", params=params)
        if not isinstance(payload, dict) or "dates" not in payload:
            raise MLBStatsError(f"Unexpected /schedule shape: keys={list(payload.keys()) if isinstance(payload, dict) else type(payload)}")

        # Flatten: schedule comes back as {dates: [{date, games: [...]}, ...]}
        games: list[dict[str, Any]] = []
        for day in payload.get("dates", []):
            for game in day.get("games", []):
                games.append(game)
        return games

    def get_probable_pitchers(self, date: str) -> list[dict[str, Any]]:
        """Get today's slate with probable pitchers hydrated.

        Returns a list of games where each has gameData.probablePitchers populated
        (when MLB has published them).
        """
        return self.get_schedule(date=date, hydrate=["probablePitcher", "team", "venue"])

    # ---------- game-level ----------

    def get_game_feed(self, game_pk: int) -> dict[str, Any]:
        """Full live/final game feed (v1.1). Big payload — has everything."""
        payload = self._get(f"/game/{game_pk}/feed/live", version="v1.1")
        if not isinstance(payload, dict):
            raise MLBStatsError(f"Expected dict from game feed, got {type(payload).__name__}")
        return payload

    def get_box_score(self, game_pk: int) -> dict[str, Any]:
        """Box score for a single game."""
        payload = self._get(f"/game/{game_pk}/boxscore")
        if not isinstance(payload, dict):
            raise MLBStatsError(f"Expected dict from boxscore, got {type(payload).__name__}")
        return payload

    def get_line_score(self, game_pk: int) -> dict[str, Any]:
        """Compact line score: innings, runs, hits, errors."""
        payload = self._get(f"/game/{game_pk}/linescore")
        if not isinstance(payload, dict):
            raise MLBStatsError(f"Expected dict from linescore, got {type(payload).__name__}")
        return payload

    # ---------- teams ----------

    def get_teams(self, season: int | str | None = None) -> list[dict[str, Any]]:
        """All 30 MLB teams."""
        params: dict[str, Any] = {"sportId": SPORT_ID_MLB}
        if season:
            params["season"] = str(season)
        payload = self._get("/teams", params=params)
        if not isinstance(payload, dict) or "teams" not in payload:
            raise MLBStatsError(f"Unexpected /teams shape")
        teams = payload["teams"]
        if not isinstance(teams, list):
            raise MLBStatsError(f"Expected list under 'teams', got {type(teams).__name__}")
        return teams

    def get_team_roster(self, team_id: int, *, roster_type: str = "active") -> list[dict[str, Any]]:
        """Roster for a team. roster_type: 'active', '40Man', 'fullSeason', etc."""
        payload = self._get(f"/teams/{team_id}/roster", params={"rosterType": roster_type})
        if not isinstance(payload, dict) or "roster" not in payload:
            raise MLBStatsError(f"Unexpected /teams/{team_id}/roster shape")
        return payload["roster"]

    # ---------- people / venues ----------

    def get_person(self, person_id: int) -> dict[str, Any]:
        """Player metadata: name, hand, position, debut, etc."""
        payload = self._get(f"/people/{person_id}")
        if not isinstance(payload, dict) or "people" not in payload:
            raise MLBStatsError(f"Unexpected /people/{person_id} shape")
        people = payload["people"]
        if not isinstance(people, list) or not people:
            raise MLBStatsError(f"Empty people response for {person_id}")
        return people[0]

    def get_venue(self, venue_id: int) -> dict[str, Any]:
        """Venue (park) metadata."""
        payload = self._get(f"/venues/{venue_id}")
        if not isinstance(payload, dict) or "venues" not in payload:
            raise MLBStatsError(f"Unexpected /venues/{venue_id} shape")
        venues = payload["venues"]
        if not isinstance(venues, list) or not venues:
            raise MLBStatsError(f"Empty venues response for {venue_id}")
        return venues[0]

    # ---------- validators ----------

    @staticmethod
    def _validate_iso_date(s: str) -> None:
        # YYYY-MM-DD with dashes
        if not (
            isinstance(s, str)
            and len(s) == 10
            and s[4] == "-"
            and s[7] == "-"
            and s[:4].isdigit()
            and s[5:7].isdigit()
            and s[8:].isdigit()
        ):
            raise ValueError(f"Expected 'YYYY-MM-DD' format, got {s!r}")
