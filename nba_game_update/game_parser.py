"""Parse Tank01 game responses into dicts for Neon upsert."""

from datetime import datetime
from typing import Optional

TEAM_MAP = {
    "GS": "GSW", "NY": "NYK", "SA": "SAS",
    "NO": "NOP", "PHO": "PHX", "BRK": "BKN", "CHO": "CHA",
}


def standardize_team(team: str) -> str:
    return TEAM_MAP.get(team, team)


def derive_season(game_date_str: str) -> str:
    if len(game_date_str) == 8 and game_date_str.isdigit():
        dt = datetime.strptime(game_date_str, "%Y%m%d")
    elif "-" in game_date_str:
        dt = datetime.strptime(game_date_str[:10], "%Y-%m-%d")
    else:
        dt = datetime.strptime(game_date_str, "%Y%m%d")
    if dt.month >= 10:
        return f"{dt.year}-{str(dt.year + 1)[2:]}"
    return f"{dt.year - 1}-{str(dt.year)[2:]}"


def parse_game(game: dict, fallback_date: str = None) -> Optional[dict]:
    game_id = game.get("gameID") or game.get("game_id")
    if not game_id:
        return None

    home_team = standardize_team(game.get("home", ""))
    away_team = standardize_team(game.get("away", ""))
    if not home_team or not away_team:
        return None

    status_code = str(game.get("gameStatusCode", ""))
    game_status = str(game.get("gameStatus", ""))
    is_completed = (
        status_code == "2"
        or "Completed" in game_status
        or "Final" in game_status
    )
    if not is_completed:
        return None

    try:
        home_score = int(game["homePts"])
        away_score = int(game["awayPts"])
    except (KeyError, TypeError, ValueError):
        return None

    raw_date = game.get("gameDate", fallback_date or "")
    if len(raw_date) == 8 and raw_date.isdigit():
        game_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
    else:
        game_date = raw_date

    return {
        "game_id": str(game_id),
        "game_date": game_date,
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "season": derive_season(raw_date),
        "status": "completed",
    }
