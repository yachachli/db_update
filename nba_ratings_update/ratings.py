"""
KenPom-style AdjEM team ratings calculator with recency weighting.

Self-contained — no external config imports.  Parameters are embedded.
"""

import math
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd

ADJEM_PARAMS = {
    "half_life_days": 30,
    "weight_floor": 0.10,
    "iterations": 5,
    "min_games": 5,
    "min_games_regression": 0.5,
}

LEAGUE_AVG_EFFICIENCY = 110.0


def _game_weight(game_date, prediction_date, half_life=30, floor=0.10):
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            gd = datetime.strptime(str(game_date), fmt)
            break
        except ValueError:
            gd = None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            pd_ = datetime.strptime(str(prediction_date), fmt)
            break
        except ValueError:
            pd_ = None
    if gd is None or pd_ is None:
        return 1.0
    days = (pd_ - gd).days
    if days < 0:
        return 0.0
    return max(math.pow(0.5, days / half_life), floor)


def _weighted_mean(vals, weights):
    tw = weights.sum()
    if tw == 0:
        return float(np.mean(vals)) if len(vals) else 0.0
    return float(np.dot(vals, weights) / tw)


def calculate_ratings(games_df: pd.DataFrame, prediction_date: str = None) -> list[dict]:
    """
    Calculate recency-weighted, opponent-adjusted AdjEM ratings.

    Returns a list of dicts: team, adj_em, adj_o, adj_d, games
    """
    df = games_df.copy()
    df["possessions"] = (df["home_score"] + df["away_score"]) / 2.0
    df["home_off_eff"] = df["home_score"] / df["possessions"] * 100
    df["away_off_eff"] = df["away_score"] / df["possessions"] * 100
    df["home_def_eff"] = df["away_score"] / df["possessions"] * 100
    df["away_def_eff"] = df["home_score"] / df["possessions"] * 100

    if prediction_date and "game_date" in df.columns:
        hl = ADJEM_PARAMS["half_life_days"]
        fl = ADJEM_PARAMS["weight_floor"]
        df["weight"] = df["game_date"].apply(
            lambda d: _game_weight(str(d).split(" ")[0], prediction_date, hl, fl)
        )
    else:
        df["weight"] = 1.0

    teams = sorted(set(df["home_team"].unique()) | set(df["away_team"].unique()))
    ratings: Dict[str, dict] = {}

    for team in teams:
        hm = df["home_team"] == team
        am = df["away_team"] == team
        off_vals = np.concatenate([
            df.loc[hm, "home_off_eff"].values,
            df.loc[am, "away_off_eff"].values,
        ])
        off_w = np.concatenate([
            df.loc[hm, "weight"].values,
            df.loc[am, "weight"].values,
        ])
        def_vals = np.concatenate([
            df.loc[hm, "home_def_eff"].values,
            df.loc[am, "away_def_eff"].values,
        ])
        raw_o = _weighted_mean(off_vals, off_w) if len(off_vals) else LEAGUE_AVG_EFFICIENCY
        raw_d = _weighted_mean(def_vals, off_w) if len(def_vals) else LEAGUE_AVG_EFFICIENCY
        ratings[team] = {
            "adj_o": raw_o, "adj_d": raw_d,
            "raw_o": raw_o, "raw_d": raw_d,
            "games": len(off_vals),
        }

    for _ in range(ADJEM_PARAMS["iterations"]):
        ao_wsum = {t: 0.0 for t in teams}
        ad_wsum = {t: 0.0 for t in teams}
        wt = {t: 0.0 for t in teams}
        for _, g in df.iterrows():
            h, a, w = g["home_team"], g["away_team"], g["weight"]
            h_exp = LEAGUE_AVG_EFFICIENCY + (ratings[a]["adj_d"] - LEAGUE_AVG_EFFICIENCY)
            a_exp = LEAGUE_AVG_EFFICIENCY + (ratings[h]["adj_d"] - LEAGUE_AVG_EFFICIENCY)
            h_adj = g["home_off_eff"] - h_exp + LEAGUE_AVG_EFFICIENCY
            a_adj = g["away_off_eff"] - a_exp + LEAGUE_AVG_EFFICIENCY
            ao_wsum[h] += h_adj * w
            ao_wsum[a] += a_adj * w
            ad_wsum[h] += a_adj * w
            ad_wsum[a] += h_adj * w
            wt[h] += w
            wt[a] += w
        for t in teams:
            if wt[t] > 0:
                ratings[t]["adj_o"] = ao_wsum[t] / wt[t]
                ratings[t]["adj_d"] = ad_wsum[t] / wt[t]

    min_g = ADJEM_PARAMS["min_games"]
    reg = ADJEM_PARAMS["min_games_regression"]
    for t in teams:
        g = ratings[t]["games"]
        if 0 < g < min_g:
            blend = reg * (g / min_g)
            ratings[t]["adj_o"] = blend * ratings[t]["adj_o"] + (1 - blend) * LEAGUE_AVG_EFFICIENCY
            ratings[t]["adj_d"] = blend * ratings[t]["adj_d"] + (1 - blend) * LEAGUE_AVG_EFFICIENCY

    results = []
    for t, r in ratings.items():
        results.append({
            "team": t,
            "adj_em": round(r["adj_o"] - r["adj_d"], 2),
            "adj_o": round(r["adj_o"], 2),
            "adj_d": round(r["adj_d"], 2),
            "games": r["games"],
        })
    return sorted(results, key=lambda x: x["adj_em"], reverse=True)
