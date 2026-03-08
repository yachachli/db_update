"""
Task 1 (Phase 1.5): Reconstruct team schedules from FanMatch data.
No KenPom schedule scraping — all from fanmatch_2026.parquet.
"""

import re
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import get_historical_dir, settings

logger = logging.getLogger(__name__)

# In-memory cache: {team_name: schedule_df}. Loaded once from parquet.
_schedule_cache: dict[str, pd.DataFrame] = {}
_fanmatch_df: Optional[pd.DataFrame] = None

# Conference abbreviations that appear at end of Game string (2-3 chars)
CONF_PATTERN = re.compile(r"^([A-Z][A-Z0-9]{1,2})$")


def parse_fanmatch_game(game_str: str) -> Optional[dict]:
    """
    Parse FanMatch game string into structured data.

    Input: "14 Alabama at 32 Georgia SEC"
    Output: {
        "away_team": "Alabama",
        "away_rank": 14,
        "home_team": "Georgia",
        "home_rank": 32,
        "conference": "SEC",
        "is_neutral": False
    }
    """
    if not game_str or not isinstance(game_str, str):
        return None
    s = game_str.strip()
    if not s:
        return None

    # Neutral: "5 Duke vs. 12 Kentucky" or "5 Duke vs 12 Kentucky"
    neutral_match = re.match(
        r"^(\d+)\s+(.+?)\s+vs\.?\s+(\d+)\s+(.+)$",
        s,
        re.IGNORECASE,
    )
    if neutral_match:
        return {
            "away_team": neutral_match.group(2).strip(),
            "away_rank": int(neutral_match.group(1)),
            "home_team": neutral_match.group(4).strip(),
            "home_rank": int(neutral_match.group(3)),
            "conference": "",
            "is_neutral": True,
        }

    # "14 Alabama at 32 Georgia SEC" — conference 2-3 uppercase at end
    at_match = re.match(r"^(\d+)\s+(.+?)\s+at\s+(\d+)\s+(.+?)\s+([A-Z][A-Z0-9]{1,2})$", s)
    if at_match:
        return {
            "away_team": at_match.group(2).strip(),
            "away_rank": int(at_match.group(1)),
            "home_team": at_match.group(4).strip(),
            "home_rank": int(at_match.group(3)),
            "conference": at_match.group(5).strip(),
            "is_neutral": False,
        }

    # Fallback: "rank Team at rank Team" without conference
    at_simple = re.match(r"^(\d+)\s+(.+?)\s+at\s+(\d+)\s+(.+)$", s)
    if at_simple:
        return {
            "away_team": at_simple.group(2).strip(),
            "away_rank": int(at_simple.group(1)),
            "home_team": at_simple.group(4).strip(),
            "home_rank": int(at_simple.group(3)),
            "conference": "",
            "is_neutral": False,
        }

    # Result line: "8 Duke 75, 39 Texas 60" — rank Name score , rank Name score.
    # Use same convention as "at" format: first = away, second = home (so home/away is consistent with Location).
    result_match = re.match(r"^(\d+)\s+(.+?)\s+(\d+)\s*,\s*(\d+)\s+(.+?)\s+(\d+)$", s)
    if result_match:
        return {
            "away_team": result_match.group(2).strip(),
            "away_rank": int(result_match.group(1)),
            "home_team": result_match.group(5).strip(),
            "home_rank": int(result_match.group(4)),
            "conference": "",
            "is_neutral": False,
        }

    logger.debug("Could not parse game string: %s", s[:80])
    return None


def parse_fanmatch_prediction(pred_str: str) -> Optional[dict]:
    """
    Parse FanMatch Prediction column. Input: "Alabama 92-91 (55%) [75]"
    Output: {
        "favored_team": "Alabama",
        "predicted_score_fav": 92,
        "predicted_score_dog": 91,
        "predicted_margin": 1,
        "win_probability": 0.55
    }
    """
    if not pred_str or not isinstance(pred_str, str):
        return None
    s = pred_str.strip()
    # Match: "TeamName 92-91 (55%)" — team name, then score, then (pct)
    m = re.match(r"^(.+?)\s+(\d+)-(\d+)\s*\((\d+)%\)", s)
    if not m:
        return None
    team = m.group(1).strip()
    score_fav = int(m.group(2))
    score_dog = int(m.group(3))
    pct = int(m.group(4)) / 100.0
    margin = score_fav - score_dog
    return {
        "favored_team": team,
        "predicted_score_fav": score_fav,
        "predicted_score_dog": score_dog,
        "predicted_margin": margin,
        "win_probability": pct,
    }


def _load_fanmatch_df() -> Optional[pd.DataFrame]:
    """Load fanmatch_2026.parquet from historical dir. Prefer backend/data/historical (env) or app/data/historical."""
    global _fanmatch_df
    if _fanmatch_df is not None:
        return _fanmatch_df
    hist_dir = get_historical_dir()
    path = hist_dir / "fanmatch_2026.parquet"
    if not path.exists():
        # Try alternate path used by .env (./data/historical)
        alt = Path(settings.HISTORICAL_DIR) if settings.HISTORICAL_DIR else Path("data/historical")
        if not alt.is_absolute():
            alt = Path.cwd() / alt
        path = alt / "fanmatch_2026.parquet"
    if not path.exists():
        return None
    _fanmatch_df = pd.read_parquet(path)
    return _fanmatch_df


def _team_matches(team_name: str, name: str) -> bool:
    """Case-insensitive match; team_name may be partial (e.g. 'Duke' vs 'Duke')."""
    if pd.isna(name):
        return False
    a = str(team_name).strip().lower()
    b = str(name).strip().lower()
    return a == b or a in b or b in a


def reconstruct_team_schedule(team_name: str, fanmatch_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Filter fanmatch_df to all games involving team_name. Returns schedule with:
    date, opponent, opponent_kenpom_rank, location, predicted_margin, predicted_win_prob,
    actual_score_team, actual_score_opp, actual_margin, covered_prediction
    """
    if fanmatch_df is None:
        fanmatch_df = _load_fanmatch_df()
    if fanmatch_df is None or len(fanmatch_df) == 0:
        return pd.DataFrame()

    team_name = str(team_name).strip()
    rows = []

    for _, row in fanmatch_df.iterrows():
        game_str = row.get("Game")
        if pd.isna(game_str):
            continue
        parsed = parse_fanmatch_game(str(game_str))
        if not parsed:
            continue
        away = parsed["away_team"]
        home = parsed["home_team"]
        if not _team_matches(team_name, away) and not _team_matches(team_name, home):
            continue

        date_val = row.get("fanmatch_date") or row.get("date")
        if pd.isna(date_val):
            continue
        date_str = str(date_val)[:10]

        if _team_matches(team_name, away):
            opponent = home
            opponent_rank = parsed["home_rank"]
            location = "neutral" if parsed["is_neutral"] else "away"
        else:
            opponent = away
            opponent_rank = parsed["away_rank"]
            location = "neutral" if parsed["is_neutral"] else "home"

        pred_mov = row.get("PredictedMOV")
        pred_win_prob = row.get("WinProbability")
        if pred_win_prob is not None and isinstance(pred_win_prob, str):
            pred_win_prob = float(pred_win_prob.replace("%", "")) / 100.0 if "%" in str(pred_win_prob) else None
        pred_win_prob = float(pred_win_prob) if pred_win_prob is not None and not pd.isna(pred_win_prob) else None
        pred_mov = float(pred_mov) if pred_mov is not None and not pd.isna(pred_mov) else None

        winner = row.get("Winner")
        loser = row.get("Loser")
        winner_score = row.get("WinnerScore")
        loser_score = row.get("LoserScore")
        actual_margin = row.get("ActualMOV")

        actual_score_team = actual_score_opp = None
        if not pd.isna(winner) and not pd.isna(loser):
            if _team_matches(team_name, winner):
                actual_score_team = _safe_float(winner_score)
                actual_score_opp = _safe_float(loser_score)
                actual_margin_val = _safe_float(actual_margin)
            elif _team_matches(team_name, loser):
                actual_score_team = _safe_float(loser_score)
                actual_score_opp = _safe_float(winner_score)
                actual_margin_val = -_safe_float(actual_margin) if actual_margin is not None else None
            else:
                actual_margin_val = None
        else:
            actual_margin_val = None

        # Predicted margin from this team's perspective (positive = team favored)
        pred_mov_team = None
        if pred_mov is not None:
            pred_mov_team = pred_mov if _team_matches(team_name, row.get("PredictedWinner", "")) else -pred_mov
        # covered_prediction: did this team beat KenPom's predicted margin?
        covered_prediction = None
        if pred_mov_team is not None and actual_margin_val is not None:
            covered_prediction = actual_margin_val >= pred_mov_team - 0.5

        rows.append({
            "date": date_str,
            "opponent": opponent,
            "opponent_kenpom_rank": opponent_rank,
            "location": location,
            "predicted_margin": pred_mov,
            "predicted_margin_team": pred_mov_team,
            "predicted_win_prob": pred_win_prob,
            "actual_score_team": actual_score_team,
            "actual_score_opp": actual_score_opp,
            "actual_margin": actual_margin_val,
            "covered_prediction": covered_prediction,
        })

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _safe_float(x, default=None):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _build_schedule_cache() -> None:
    """Build in-memory cache of all team schedules from FanMatch parquet."""
    global _schedule_cache
    df = _load_fanmatch_df()
    if df is None or len(df) == 0:
        return
    teams = set()
    for _, row in df.iterrows():
        parsed = parse_fanmatch_game(str(row.get("Game", "")))
        if parsed:
            teams.add(parsed["away_team"])
            teams.add(parsed["home_team"])
    for team in teams:
        _schedule_cache[team] = reconstruct_team_schedule(team, df)
    logger.info("Schedule cache built for %d teams", len(_schedule_cache))


def get_team_schedule(team_name: str) -> Optional[pd.DataFrame]:
    """
    Return reconstructed schedule for team. Uses in-memory cache (built on first use).
    Called by /api/teams/{team_name}/schedule.
    """
    if not _schedule_cache and _load_fanmatch_df() is not None:
        _build_schedule_cache()
    team_name = str(team_name).strip()
    for key, sched in _schedule_cache.items():
        if _team_matches(team_name, key):
            return sched
    # Try building schedule on the fly in case team name differs slightly
    sched = reconstruct_team_schedule(team_name, _fanmatch_df)
    if len(sched) > 0:
        return sched
    return None
