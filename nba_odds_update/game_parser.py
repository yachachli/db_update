"""Season derivation helper (shared with game update module)."""

from datetime import datetime


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
