"""
Task 2 (Phase 1.5): Recency metrics from reconstructed schedules.
Compute recent_adj_oe, recent_adj_de, record, trend_direction for prediction model.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

LEAGUE_AVG_DE = 100.0
LEAGUE_AVG_OE = 100.0


def _safe_float(x, default=None):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _team_matches(team_name: str, name: str) -> bool:
    if pd.isna(name):
        return False
    a = str(team_name).strip().lower()
    b = str(name).strip().lower()
    return a == b or a in b or b in a


def _get_team_ratings(team_name: str, kenpom_ratings: pd.DataFrame) -> Optional[dict]:
    """Get AdjO, AdjD, AdjT for team from pomeroy-style dataframe (uses central resolver for name matching)."""
    if kenpom_ratings is None or len(kenpom_ratings) == 0:
        return None
    from app.services.team_name_resolver import find_team_row
    row = find_team_row(kenpom_ratings, team_name)
    if row is None:
        return None
    return {
        "adj_oe": _safe_float(row.get("AdjO") or row.get("AdjOE"), 100.0),
        "adj_de": _safe_float(row.get("AdjD") or row.get("AdjDE"), 100.0),
        "adj_tempo": _safe_float(row.get("AdjT") or row.get("AdjTempo"), 67.0),
    }


def calculate_recency_metrics(
    team_name: str,
    schedule_df: pd.DataFrame,
    kenpom_ratings: pd.DataFrame,
    window_days: int = 21,
) -> Optional[dict]:
    """
    Compute recent performance metrics from schedule and KenPom ratings.

    Returns: recent_adj_oe, recent_adj_de, recent_record, recent_ats_record,
             recent_margin_vs_expected, trend_direction, games_in_window.
    Returns None if 0 games in window (or low sample).
    """
    if schedule_df is None or len(schedule_df) == 0:
        return None
    team_name = str(team_name).strip()
    if "date" not in schedule_df.columns:
        return None

    # Filter to last `window_days` days (use today as reference)
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=window_days)
    schedule_df = schedule_df.copy()
    schedule_df["_date"] = pd.to_datetime(schedule_df["date"], errors="coerce").dt.date
    recent = schedule_df[schedule_df["_date"] >= cutoff].copy()
    if len(recent) == 0:
        return None

    team_ratings = _get_team_ratings(team_name, kenpom_ratings)
    if team_ratings is None:
        team_ratings = {"adj_oe": 100.0, "adj_de": 100.0, "adj_tempo": 67.0}

    oe_list = []
    de_list = []
    margins = []
    predicted_margins = []
    wins = 0
    covers = 0

    for _, row in recent.iterrows():
        opp = row.get("opponent")
        if pd.isna(opp):
            continue
        opp_ratings = _get_team_ratings(opp, kenpom_ratings)
        opp_adj_de = _safe_float(opp_ratings["adj_de"], LEAGUE_AVG_DE) if opp_ratings else LEAGUE_AVG_DE
        opp_adj_oe = _safe_float(opp_ratings["adj_oe"], LEAGUE_AVG_OE) if opp_ratings else LEAGUE_AVG_OE
        pred_mov = row.get("predicted_margin")
        actual_margin = row.get("actual_margin")
        score_team = row.get("actual_score_team")
        score_opp = row.get("actual_score_opp")
        covered = row.get("covered_prediction")

        if pred_mov is not None:
            predicted_margins.append(float(pred_mov))
        if actual_margin is not None:
            margins.append(float(actual_margin))
            if float(actual_margin) > 0:
                wins += 1
        if covered is True:
            covers += 1
        elif covered is False:
            pass  # covers stays

        # Estimate possessions: use predicted total if available, else ~70
        # From schedule we don't have predicted total per game; use (score_team + score_opp) and tempo
        if score_team is not None and score_opp is not None:
            total = float(score_team) + float(score_opp)
            # Rough possessions: total / 2 * (100 / avg_oe) approximation
            est_poss = total / 2.0 if total else 70.0
            raw_oe = (float(score_team) / est_poss * 100) if est_poss else 100.0
            raw_de = (float(score_opp) / est_poss * 100) if est_poss else 100.0
            # Opponent-adjust
            adj_oe = raw_oe * (LEAGUE_AVG_DE / opp_adj_de) if opp_adj_de else raw_oe
            adj_de = raw_de * (LEAGUE_AVG_OE / opp_adj_oe) if opp_adj_oe else raw_de
            oe_list.append(adj_oe)
            de_list.append(adj_de)

    games_in_window = len(recent)
    recent_record = f"{wins}-{games_in_window - wins}" if games_in_window else "0-0"
    covers_misses = sum(1 for _, r in recent.iterrows() if r.get("covered_prediction") is True) 
    covers_misses += sum(1 for _, r in recent.iterrows() if r.get("covered_prediction") is False)
    recent_ats_record = f"{covers}-{covers_misses - covers}" if covers_misses else "0-0"

    # Use predicted_margin_team or predicted_margin so schedule from schedule_service (predicted_margin_team) is supported
    diffs = []
    for _, r in recent.iterrows():
        am = r.get("actual_margin")
        pm = r.get("predicted_margin_team") or r.get("predicted_margin")
        if am is not None and pm is not None:
            diffs.append(float(am) - float(pm))
    recent_margin_vs_expected = sum(diffs) / len(diffs) if diffs else 0.0

    if recent_margin_vs_expected > 2:
        trend_direction = "rising"
    elif recent_margin_vs_expected < -2:
        trend_direction = "falling"
    else:
        trend_direction = "stable"

    recent_adj_oe = sum(oe_list) / len(oe_list) if oe_list else team_ratings["adj_oe"]
    recent_adj_de = sum(de_list) / len(de_list) if de_list else team_ratings["adj_de"]

    return {
        "recent_adj_oe": round(recent_adj_oe, 1),
        "recent_adj_de": round(recent_adj_de, 1),
        "recent_record": recent_record,
        "recent_ats_record": recent_ats_record,
        "recent_margin_vs_expected": round(recent_margin_vs_expected, 1),
        "trend_direction": trend_direction,
        "games_in_window": games_in_window,
    }
