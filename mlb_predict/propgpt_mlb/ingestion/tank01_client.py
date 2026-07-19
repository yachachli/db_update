"""Tank01 MLB API client.

Thin wrapper around the Tank01 MLB endpoints on RapidAPI. Returns parsed
dicts/lists. Does NOT write to the database — that's a separate concern
handled by the writers module (next step).

Endpoints implemented (Phase 1 set — extend as needed):
- get_teams(): /getMLBTeams
- get_team_roster(team_abv): /getMLBTeamRoster
- get_player_list(): /getMLBPlayerList
- get_games_for_date(yyyymmdd): /getMLBGamesForDate
- get_box_score(game_id): /getMLBBoxScore
- get_team_schedule(team_abv, season): /getMLBTeamSchedule
- get_betting_odds(yyyymmdd): /getMLBBettingOdds

Glossary:
- gameID format: 'AWY@HMA_YYYYMMDD' (e.g. 'NYY@LAD_20260514')
- gameDate format: 'YYYYMMDD'
- All responses have shape {"statusCode": 200, "body": <data>, "error"?: <msg>}
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from dotenv import load_dotenv

from .http import HttpError, request_with_retry

load_dotenv()

logger = logging.getLogger(__name__)

TANK01_BASE_URL = "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com"
TANK01_HOST = "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com"


class Tank01Error(RuntimeError):
    """Raised when Tank01 returns an error envelope or unexpected shape."""


class TankClient:
    """Tank01 MLB API client.

    Reads TANK01_API_KEY from env. Inserts a small delay between calls to
    be polite to the API and stay under per-second rate limits on lower plans.
    """

    def __init__(
        self,
        api_key: str | None = None,
        polite_delay_sec: float = 0.25,
        timeout: float = 15.0,
    ):
        self.api_key = api_key or os.getenv("TANK01_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "TANK01_API_KEY is not set. Add it to .env."
            )
        self.polite_delay_sec = polite_delay_sec
        self.timeout = timeout
        self._last_call_at = 0.0

    # ---------- internals ----------

    def _headers(self) -> dict[str, str]:
        return {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": TANK01_HOST,
            "Accept": "application/json",
        }

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.polite_delay_sec:
            time.sleep(self.polite_delay_sec - elapsed)
        self._last_call_at = time.monotonic()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Issue a GET to Tank01 and return the parsed body.

        Tank01 returns 200 even on error — we detect via the `error` key in
        the response JSON.
        """
        self._throttle()
        url = f"{TANK01_BASE_URL}{path}"
        logger.debug("Tank01 GET %s params=%s", path, params)
        response = request_with_retry(
            "GET",
            url,
            headers=self._headers(),
            params=params,
            timeout=self.timeout,
        )
        try:
            payload = response.json()
        except ValueError as e:
            raise Tank01Error(f"Tank01 returned non-JSON for {path}: {e}") from e

        if isinstance(payload, dict) and payload.get("error"):
            raise Tank01Error(
                f"Tank01 error on {path}: {payload.get('error')} (statusCode={payload.get('statusCode')})"
            )
        return payload.get("body") if isinstance(payload, dict) else payload

    # ---------- endpoints ----------

    def get_teams(
        self,
        *,
        team_stats: bool = False,
        top_performers: bool = False,
        rosters: bool = False,
    ) -> list[dict[str, Any]]:
        """List all 30 MLB teams. Optional flags pull extra data (slower).

        Returns a list of team dicts (Tank01 returns a list in `body`).
        """
        params: dict[str, Any] = {}
        if team_stats:
            params["teamStats"] = "true"
        if top_performers:
            params["topPerformers"] = "true"
        if rosters:
            params["rosters"] = "true"
        body = self._get("/getMLBTeams", params=params or None)
        if not isinstance(body, list):
            raise Tank01Error(f"Expected list from /getMLBTeams, got {type(body).__name__}")
        return body

    def get_team_roster(
        self,
        *,
        team_abv: str | None = None,
        team_id: str | None = None,
        get_stats: bool = False,
        archive_date: str | None = None,
    ) -> dict[str, Any]:
        """Roster for a single team. Provide either team_abv OR team_id."""
        if not team_abv and not team_id:
            raise ValueError("Provide team_abv or team_id")
        params: dict[str, Any] = {}
        if team_abv:
            params["teamAbv"] = team_abv
        if team_id:
            params["teamID"] = team_id
        if get_stats:
            params["getStats"] = "true"
        if archive_date:
            params["archiveDate"] = archive_date
        body = self._get("/getMLBTeamRoster", params=params)
        if not isinstance(body, dict):
            raise Tank01Error(f"Expected dict from /getMLBTeamRoster, got {type(body).__name__}")
        return body

    def get_player_list(self) -> list[dict[str, Any]]:
        """All known MLB players."""
        body = self._get("/getMLBPlayerList")
        if not isinstance(body, list):
            raise Tank01Error(f"Expected list from /getMLBPlayerList, got {type(body).__name__}")
        return body

    def get_games_for_date(self, game_date: str) -> list[dict[str, Any]]:
        """All games on a given date.

        game_date must be 'YYYYMMDD' (no dashes).
        """
        self._validate_yyyymmdd(game_date)
        body = self._get("/getMLBGamesForDate", params={"gameDate": game_date})
        if isinstance(body, dict):
            # Tank01 sometimes returns a dict keyed by gameID — normalize to list of values
            return list(body.values())
        if isinstance(body, list):
            return body
        raise Tank01Error(f"Unexpected body type from /getMLBGamesForDate: {type(body).__name__}")

    def get_box_score(self, game_id: str) -> dict[str, Any]:
        """Box score for a single game."""
        body = self._get("/getMLBBoxScore", params={"gameID": game_id})
        if not isinstance(body, dict):
            raise Tank01Error(f"Expected dict from /getMLBBoxScore, got {type(body).__name__}")
        return body

    def get_team_schedule(self, team_abv: str, season: int | str) -> dict[str, Any]:
        """Full season schedule for a team."""
        body = self._get(
            "/getMLBTeamSchedule",
            params={"teamAbv": team_abv, "season": str(season)},
        )
        if not isinstance(body, dict):
            raise Tank01Error(f"Expected dict from /getMLBTeamSchedule, got {type(body).__name__}")
        return body

    def get_betting_odds(self, game_date: str) -> dict[str, Any]:
        """Sportsbook odds snapshot for all games on a date."""
        self._validate_yyyymmdd(game_date)
        body = self._get("/getMLBBettingOdds", params={"gameDate": game_date})
        if not isinstance(body, dict):
            raise Tank01Error(f"Expected dict from /getMLBBettingOdds, got {type(body).__name__}")
        return body

    # ---------- validators ----------

    @staticmethod
    def _validate_yyyymmdd(s: str) -> None:
        if not (isinstance(s, str) and len(s) == 8 and s.isdigit()):
            raise ValueError(f"Expected gameDate in 'YYYYMMDD' format, got {s!r}")
