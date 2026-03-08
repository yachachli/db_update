"""
BracketIQ Phase 1.75 — Model Analysis & Edge Discovery.
Uses ats_complete_2026.parquet and cached KenPom data. Run from backend: py -m scripts.model_analysis
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np
from scipy import stats

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))


def _find_team_row(df: pd.DataFrame, team_name: str, name_col: str = "Team") -> pd.Series | None:
    """Use central resolver so KenPom aliases (e.g. Tennessee Martin / UT Martin) match."""
    from app.services.team_name_resolver import find_team_row as resolver_find_team_row
    return resolver_find_team_row(df, team_name, name_col=name_col)


def load_ats_complete() -> pd.DataFrame:
    """Load ats_complete_2026.parquet from historical dir."""
    for base in [_backend_root / "data" / "historical", _backend_root / "app" / "data" / "historical"]:
        path = base / "ats_complete_2026.parquet"
        if path.exists():
            return pd.read_parquet(path)
    raise FileNotFoundError("ats_complete_2026.parquet not found. Run build_ats_dataset first.")


def load_kenpom_ratings() -> pd.DataFrame | None:
    """Pomeroy ratings (AdjO, AdjD, AdjT, Rk, Conf, Team)."""
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    files = list(cache_dir.glob("pomeroy_ratings_*.parquet"))
    if not files:
        return None
    return pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))


def load_four_factors() -> pd.DataFrame | None:
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    files = list(cache_dir.glob("fourfactors_*.parquet"))
    if not files:
        return None
    return pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))


def load_teamstats() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    off_files = list(cache_dir.glob("teamstats_off_*.parquet"))
    def_files = list(cache_dir.glob("teamstats_def_*.parquet"))
    off = pd.read_parquet(max(off_files, key=lambda p: p.stat().st_mtime)) if off_files else None
    def_ = pd.read_parquet(max(def_files, key=lambda p: p.stat().st_mtime)) if def_files else None
    return off, def_


def load_height() -> pd.DataFrame | None:
    from app.config import get_cache_dir
    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    files = list(cache_dir.glob("height_*.parquet"))
    if not files:
        return None
    return pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))


def _get_team_conf(team: str, pomeroy: pd.DataFrame | None) -> str:
    if pomeroy is None:
        return ""
    row = _find_team_row(pomeroy, team)
    return str(row.get("Conf", "")).strip() if row is not None else ""


def _get_team_rating(team: str, pomeroy: pd.DataFrame | None, col: str, default: float = 0.0) -> float:
    """Use central resolver for consistent KenPom lookup."""
    from app.services.team_name_resolver import get_rating as resolver_get_rating
    if pomeroy is None:
        return default
    return resolver_get_rating(pomeroy, team, col, default)


def _get_team_rank(team: str, pomeroy: pd.DataFrame | None) -> int:
    if pomeroy is None:
        return 999
    row = _find_team_row(pomeroy, team)
    if row is None:
        return 999
    v = row.get("Rk")
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 999
    try:
        return int(v)
    except (TypeError, ValueError):
        return 999


def _get_ff(team: str, ff: pd.DataFrame | None, key: str) -> float:
    if ff is None:
        return 0.0
    row = _find_team_row(ff, team)
    if row is None:
        return 0.0
    v = row.get(key)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0.0
    try:
        f = float(v)
        if f > 1 and "%" in key:
            f /= 100.0
        return f
    except (TypeError, ValueError):
        return 0.0


def _get_ts(team: str, tso: pd.DataFrame | None, col: str) -> float:
    if tso is None:
        return 0.0
    row = _find_team_row(tso, team, "Team")
    if row is None:
        return 0.0
    v = row.get(col)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 0.0
    try:
        f = float(v)
        if f > 1:
            f /= 100.0
        return f
    except (TypeError, ValueError):
        return 0.0


def _get_height_exp(team: str, height_df: pd.DataFrame | None) -> tuple[float, float]:
    if height_df is None:
        return 0.0, 0.0
    row = _find_team_row(height_df, team)
    if row is None:
        return 0.0, 0.0
    h = float(row.get("AvgHgt", 0) or 0)
    e = float(row.get("Experience", 0) or 0)
    return h, e


# ---------- Analysis 1: Conference accuracy ----------
def analyze_conference_accuracy(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    out_rows = []
    # Add conference to each game (home team's conference as game conference for bucketing)
    ats = ats_df.copy()
    if kenpom_df is not None:
        ats["home_conf"] = ats["home_team"].map(lambda t: _get_team_conf(t, kenpom_df))
        ats["away_conf"] = ats["away_team"].map(lambda t: _get_team_conf(t, kenpom_df))
        ats["intra"] = (ats["home_conf"] == ats["away_conf"]) & (ats["home_conf"] != "")
    else:
        ats["home_conf"] = ats["away_conf"] = ""
        ats["intra"] = False

    confs = list(set(ats["home_conf"].dropna().tolist() + ats["away_conf"].dropna().tolist()))
    confs = [c for c in confs if c and str(c).strip()]
    if not confs:
        confs = ["(no conference data)"]

    for conf in sorted(confs):
        if conf == "(no conference data)":
            sub = ats[(ats["home_conf"].fillna("") == "") & (ats["away_conf"].fillna("") == "")]
        else:
            sub = ats[(ats["home_conf"] == conf) | (ats["away_conf"] == conf)]
        if len(sub) < 5:
            continue
        kp_mov = sub["kenpom_predicted_margin"].dropna()
        mae = float(np.abs(sub["actual_margin_home"] - sub["kenpom_predicted_margin"]).mean()) if len(kp_mov) else None
        # Correct pick: predicted winner actually won (sign of pred margin vs actual)
        pred_winner_right = (np.sign(sub["kenpom_predicted_margin"]) == np.sign(sub["actual_margin_home"]))
        correct_pct = round(100.0 * pred_winner_right.mean(), 1) if len(pred_winner_right) else None
        kp_cover = sub["covered_kenpom"].dropna()
        kp_cover_rate = round(100.0 * kp_cover.mean(), 1) if len(kp_cover) else None
        veg_cover = sub["covered_vegas"].dropna()
        veg_cover_rate = round(100.0 * veg_cover.mean(), 1) if len(veg_cover) else None
        edge = sub["kenpom_vs_vegas_edge"].dropna()
        avg_edge = round(float(edge.mean()), 2) if len(edge) else None
        # KenPom edge over Vegas: when they disagree, who's right more often?
        disagree = sub["covered_kenpom"].notna() & sub["covered_vegas"].notna()
        disagree = sub.loc[disagree, ["covered_kenpom", "covered_vegas"]]
        disagree = disagree[disagree["covered_kenpom"] != disagree["covered_vegas"]]
        kp_wins = (disagree["covered_kenpom"] == True).sum() if len(disagree) else 0
        veg_wins = (disagree["covered_vegas"] == True).sum() if len(disagree) else 0
        total_d = kp_wins + veg_wins
        kp_edge_over_vegas = round(100.0 * kp_wins / total_d, 1) if total_d else None

        out_rows.append({
            "conference": conf,
            "n_games": len(sub),
            "kenpom_mae": round(mae, 2) if mae is not None else None,
            "kenpom_correct_pick_pct": correct_pct,
            "kenpom_ats_cover_pct": kp_cover_rate,
            "vegas_ats_cover_pct": veg_cover_rate,
            "kp_edge_over_vegas_when_disagree_pct": kp_edge_over_vegas,
            "avg_kenpom_vs_vegas_edge": avg_edge,
        })

    # Sort by MAE ascending (best first)
    out_rows.sort(key=lambda r: (r["kenpom_mae"] or 999))

    # Intra vs inter
    if "intra" in ats.columns and ats["intra"].any():
        intra = ats[ats["intra"]]
        inter = ats[~ats["intra"]]
        intra_mae = round(float(np.abs(intra["actual_margin_home"] - intra["kenpom_predicted_margin"]).mean()), 2) if len(intra) else None
        inter_mae = round(float(np.abs(inter["actual_margin_home"] - inter["kenpom_predicted_margin"]).mean()), 2) if len(inter) else None
        summary = {"intra_conf_mae": intra_mae, "inter_conf_mae": inter_mae, "intra_n": len(intra), "inter_n": len(inter)}
    else:
        summary = {}

    out_path = _backend_root / "data" / "analysis" / "conference_accuracy.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(out_path, index=False)
    return {"rows": out_rows, "summary": summary}


# ---------- Analysis 2: Totals correlation ----------
def analyze_totals_correlation(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    ats = ats_df[ats_df["over_under_result"].notna()].copy()
    if ats.empty:
        out_path = _backend_root / "data" / "analysis" / "totals_correlations.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return {}

    if kenpom_df is not None:
        ats["home_adjoe"] = ats["home_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjO", 100))
        ats["away_adjoe"] = ats["away_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjO", 100))
        ats["home_adjt"] = ats["home_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjT", 68))
        ats["away_adjt"] = ats["away_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjT", 68))
        ats["home_adjde"] = ats["home_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjD", 100))
        ats["away_adjde"] = ats["away_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjD", 100))
    else:
        for c in ["home_adjoe", "away_adjoe", "home_adjt", "away_adjt", "home_adjde", "away_adjde"]:
            ats[c] = 100.0

    ats["combined_adjoe"] = ats["home_adjoe"] + ats["away_adjoe"]
    ats["combined_adjt"] = ats["home_adjt"] + ats["away_adjt"]
    ats["combined_adjde"] = ats["home_adjde"] + ats["away_adjde"]
    ats["is_over"] = (ats["over_under_result"] == "over").astype(float)

    rows = []
    # 1) Combined AdjOE vs over rate
    if "combined_adjoe" in ats.columns:
        q1 = ats["combined_adjoe"].quantile(0.25)
        q3 = ats["combined_adjoe"].quantile(0.75)
        top = ats[ats["combined_adjoe"] >= q3]
        bot = ats[ats["combined_adjoe"] <= q1]
        r, p = stats.pearsonr(ats["combined_adjoe"], ats["is_over"]) if len(ats) > 2 else (None, None)
        rows.append({"metric": "combined_AdjOE_vs_over", "pearson_r": round(r, 3) if r is not None else None, "p_value": round(p, 4) if p is not None else None,
                     "top_quartile_over_pct": round(100.0 * top["is_over"].mean(), 1) if len(top) else None,
                     "bottom_quartile_over_pct": round(100.0 * bot["is_over"].mean(), 1) if len(bot) else None, "actionable": abs(r) > 0.15 if r else False})

    # 2) Combined AdjT vs over
    if "combined_adjt" in ats.columns:
        r, p = stats.pearsonr(ats["combined_adjt"], ats["is_over"]) if len(ats) > 2 else (None, None)
        fast_mask = (ats["home_adjt"].rank(pct=True) > 0.8) & (ats["away_adjt"].rank(pct=True) > 0.8)
        slow_mask = (ats["home_adjt"].rank(pct=True) < 0.2) & (ats["away_adjt"].rank(pct=True) < 0.2)
        fast = ats[fast_mask]
        slow = ats[slow_mask]
        rows.append({"metric": "combined_AdjTempo_vs_over", "pearson_r": round(r, 3) if r is not None else None, "p_value": round(p, 4) if p is not None else None,
                     "fast_both_over_pct": round(100.0 * fast["is_over"].mean(), 1) if len(fast) else None,
                     "slow_both_under_pct": round(100.0 * (1 - slow["is_over"].mean()), 1) if len(slow) else None, "actionable": abs(r) > 0.15 if r else False})

    # 3) Combined AdjDE vs under
    if "combined_adjde" in ats.columns:
        r, p = stats.pearsonr(ats["combined_adjde"], 1 - ats["is_over"]) if len(ats) > 2 else (None, None)
        rows.append({"metric": "combined_AdjDE_vs_under", "pearson_r": round(r, 3) if r is not None else None, "p_value": round(p, 4) if p is not None else None, "actionable": abs(r) > 0.15 if r else False})

    # 7) Vegas total MAE by tempo tier
    if "vegas_total" in ats.columns and "actual_total" in ats.columns:
        ats["vegas_total_mae"] = np.abs(ats["actual_total"] - ats["vegas_total"])
        if "combined_adjt" in ats.columns:
            ats["tempo_tier"] = pd.qcut(ats["combined_adjt"], 3, labels=["slow", "mid", "fast"], duplicates="drop")
            mae_by_tempo = ats.groupby("tempo_tier", observed=True)["vegas_total_mae"].mean()
            for tier in mae_by_tempo.index:
                rows.append({"metric": f"vegas_total_MAE_{tier}", "vegas_mae": round(float(mae_by_tempo[tier]), 2), "actionable": False})

    out_path = _backend_root / "data" / "analysis" / "totals_correlations.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows}


# ---------- Analysis 3: Spread by profile ----------
def analyze_spread_by_profile(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    ats = ats_df[ats_df["covered_vegas"].notna()].copy()
    if ats.empty or kenpom_df is None:
        out_path = _backend_root / "data" / "analysis" / "spread_profiles.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return {}

    # Attach home/away ratings and ranks
    ats["home_rank"] = ats["home_team"].map(lambda t: _get_team_rank(t, kenpom_df))
    ats["away_rank"] = ats["away_team"].map(lambda t: _get_team_rank(t, kenpom_df))
    ats["home_adjt"] = ats["home_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjT", 68))
    ats["away_adjt"] = ats["away_team"].map(lambda t: _get_team_rating(t, kenpom_df, "AdjT", 68))
    n_teams = len(kenpom_df)
    ats["home_rank_pct"] = 1 - (ats["home_rank"] - 1).clip(0) / max(n_teams, 1)
    ats["away_rank_pct"] = 1 - (ats["away_rank"] - 1).clip(0) / max(n_teams, 1)
    # Vegas spread from home POV: negative = home favored
    ats["vegas_spread_abs"] = ats["vegas_spread"].abs()
    ats["home_fav"] = ats["vegas_spread"] < 0
    ats["home_underdog"] = ats["vegas_spread"] > 0

    ff = load_four_factors()
    tso, _ = load_teamstats()
    height_df = load_height()
    rows = []

    def add_row(profile: str, mask: pd.Series, cover_series: pd.Series):
        sub = cover_series[mask]
        if len(sub) < 10:
            return
        n = len(sub)
        covers = sub.sum()
        pct = round(100.0 * covers / n, 1)
        # z-test for proportion vs 0.5
        z = (covers / n - 0.5) / (0.5 * (0.5 / n) ** 0.5) if n else 0
        p_val = 2 * (1 - stats.norm.cdf(abs(z))) if n else 1.0
        actionable = (n >= 30 and (pct > 55 or pct < 45)) and p_val < 0.05
        rows.append({"profile": profile, "n": n, "cover_pct": pct, "p_value": round(p_val, 4), "actionable": actionable})

    # Fast teams as favorites / underdogs (use home team tempo for "home is fast" etc.)
    fast_thresh = ats["home_adjt"].quantile(0.8) if len(ats) else 70
    slow_thresh = ats["home_adjt"].quantile(0.2) if len(ats) else 65
    add_row("home_fast_favorite", ats["home_fav"] & (ats["home_adjt"] >= fast_thresh), ats["covered_vegas"])
    add_row("home_fast_underdog", ats["home_underdog"] & (ats["home_adjt"] >= fast_thresh), ats["covered_vegas"])
    add_row("home_slow_favorite", ats["home_fav"] & (ats["home_adjt"] <= slow_thresh), ats["covered_vegas"])
    add_row("home_slow_underdog", ats["home_underdog"] & (ats["home_adjt"] <= slow_thresh), ats["covered_vegas"])

    # Elite defense (top 25 rank) underdog
    elite_def_rank = 25
    home_elite_d = ats["home_rank"] <= elite_def_rank
    away_elite_d = ats["away_rank"] <= elite_def_rank
    add_row("home_elite_def_underdog", ats["home_underdog"] & home_elite_d, ats["covered_vegas"])
    add_row("away_elite_def_underdog", ats["home_fav"] & away_elite_d, ~ats["covered_vegas"])

    # Close games spread < 5
    close = ats["vegas_spread_abs"] < 5
    add_row("close_game_home_cover", close, ats["covered_vegas"])

    out_path = _backend_root / "data" / "analysis" / "spread_profiles.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows}


# ---------- Analysis 4: KenPom vs Vegas edge ----------
def analyze_kenpom_vs_vegas_edge(ats_df: pd.DataFrame) -> dict:
    ats = ats_df[ats_df["kenpom_vs_vegas_edge"].notna()].copy()
    if ats.empty:
        out_path = _backend_root / "data" / "analysis" / "edge_analysis.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return {}

    ats["edge_abs"] = ats["kenpom_vs_vegas_edge"].abs()
    rows = []

    # 1) Edge buckets
    for low, high, label in [(0, 1, "agree"), (1, 3, "mild"), (3, 5, "significant"), (5, 99, "major")]:
        sub = ats[(ats["edge_abs"] >= low) & (ats["edge_abs"] < high)]
        if len(sub) < 5:
            continue
        kp_cover = sub["covered_kenpom"].sum()
        tot = sub["covered_kenpom"].notna().sum()
        rows.append({"edge_bucket": label, "edge_min": low, "edge_max": high, "n": len(sub), "kenpom_cover_rate_pct": round(100.0 * kp_cover / tot, 1) if tot else None})

    # 2) Directional
    pos = ats[ats["kenpom_vs_vegas_edge"] > 0]
    neg = ats[ats["kenpom_vs_vegas_edge"] < 0]
    rows.append({"edge_bucket": "KenPom_likes_home_more", "n": len(pos), "home_cover_rate_pct": round(100.0 * pos["covered_vegas"].mean(), 1) if len(pos) else None})
    rows.append({"edge_bucket": "KenPom_likes_away_more", "n": len(neg), "away_cover_rate_pct": round(100.0 * (1 - neg["covered_vegas"].mean()), 1) if len(neg) else None})

    # 3) Threshold profitability
    for x in [1, 2, 3, 4, 5]:
        sub = ats[ats["edge_abs"] > x]
        if len(sub) < 10:
            continue
        kc = sub["covered_kenpom"].sum()
        tot = sub["covered_kenpom"].notna().sum()
        rows.append({"edge_bucket": f"|edge|>{x}", "n": len(sub), "kenpom_cover_rate_pct": round(100.0 * kc / tot, 1) if tot else None})

    # 4) By time of season
    ats["month"] = pd.to_datetime(ats["game_date"], errors="coerce").dt.month
    early = ats[ats["month"].isin([11, 12])]
    late = ats[ats["month"].isin([2, 3])]
    if len(early) >= 20:
        kc = early["covered_kenpom"].sum()
        tot = early["covered_kenpom"].notna().sum()
        rows.append({"edge_bucket": "early_season_NovDec", "n": len(early), "kenpom_cover_rate_pct": round(100.0 * kc / tot, 1) if tot else None})
    if len(late) >= 20:
        kc = late["covered_kenpom"].sum()
        tot = late["covered_kenpom"].notna().sum()
        rows.append({"edge_bucket": "late_season_FebMar", "n": len(late), "kenpom_cover_rate_pct": round(100.0 * kc / tot, 1) if tot else None})

    # 5) By spread size
    big_fav = ats[ats["vegas_spread"].abs() > 10]
    toss = ats[ats["vegas_spread"].abs() < 3]
    mid = ats[(ats["vegas_spread"].abs() >= 3) & (ats["vegas_spread"].abs() <= 10)]
    for label, sub in [("spread_gt10", big_fav), ("spread_lt3", toss), ("spread_3_10", mid)]:
        if len(sub) < 10:
            continue
        kc = sub["covered_kenpom"].sum()
        tot = sub["covered_kenpom"].notna().sum()
        rows.append({"edge_bucket": label, "n": len(sub), "kenpom_cover_rate_pct": round(100.0 * kc / tot, 1) if tot else None})

    out_path = _backend_root / "data" / "analysis" / "edge_analysis.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows}


# ---------- Analysis 5: Upset patterns ----------
def analyze_upset_patterns(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    # Upset = Vegas underdog wins outright. Vegas spread from home POV: negative = home favored, so underdog = home when spread > 0 (away favored) or away when spread < 0 (home favored).
    ats = ats_df.copy()
    ats["home_favored"] = ats["vegas_spread"] < 0
    ats["underdog_won"] = (
        (ats["home_favored"] & (ats["actual_margin_home"] < 0)) |
        (~ats["home_favored"] & (ats["actual_margin_home"] > 0))
    )
    upsets = ats[ats["underdog_won"]]
    rows = []

    # 1) Upset rate by spread bucket
    for low, high in [(1, 3), (3, 6), (6, 10), (10, 15), (15, 30)]:
        sub = ats[(ats["vegas_spread"].abs() >= low) & (ats["vegas_spread"].abs() < high)]
        if len(sub) < 5:
            continue
        rate = round(100.0 * sub["underdog_won"].sum() / len(sub), 1)
        rows.append({"spread_bucket": f"{low}-{high}", "n": len(sub), "upset_rate_pct": rate})

    # 5) KenPom disagreement in upsets
    if "kenpom_vs_vegas_edge" in ats.columns:
        # When underdog won: was KenPom more on underdog side (edge toward underdog)?
        # Home underdog: vegas_spread > 0, so KenPom liking home more = positive edge. So edge > 0 when home is underdog means KenPom liked underdog.
        ats["edge_toward_underdog"] = (
            (ats["home_favored"] & (ats["kenpom_vs_vegas_edge"] < 0)) |  # away underdog, negative edge = KenPom likes away
            (~ats["home_favored"] & (ats["kenpom_vs_vegas_edge"] > 0))   # home underdog, positive edge = KenPom likes home
        )
        sub = ats[ats["underdog_won"]]
        if len(sub) >= 10:
            kp_toward = sub["edge_toward_underdog"].sum()
            rows.append({"spread_bucket": "upsets_KenPom_toward_underdog", "n": len(sub), "pct_KenPom_agreed": round(100.0 * kp_toward / len(sub), 1)})

    out_path = _backend_root / "data" / "analysis" / "upset_patterns.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows, "n_upsets": len(upsets)}


# ---------- Analysis 6: Venue effects ----------
def analyze_venue_effects(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    ats = ats_df.copy()
    rows = []
    # 1) Actual HCA
    avg_home_margin = round(float(ats["actual_margin_home"].mean()), 2)
    rows.append({"metric": "actual_avg_home_margin", "value": avg_home_margin, "note": "KenPom uses ~3.75 HCA"})

    # 2) Neutral site: we don't have flag in ATS; skip or note
    rows.append({"metric": "neutral_site", "value": None, "note": "Neutral site flag not in ATS dataset"})

    # 3) Road underdogs (away team is underdog when home favored, i.e. vegas_spread < 0)
    road_dog = ats[ats["vegas_spread"] < 0]  # home favored => away is road underdog
    if len(road_dog) >= 10:
        cover = (1 - road_dog["covered_vegas"]).sum()  # away covered when home did not
        rows.append({"metric": "road_underdog_cover_pct", "value": round(100.0 * cover / len(road_dog), 1), "n": len(road_dog)})

    # 4) Home favorites by spread size
    home_fav = ats[ats["vegas_spread"] < 0]
    for low, high in [(1, 5), (5, 10), (10, 99)]:
        sub = home_fav[(home_fav["vegas_spread"].abs() >= low) & (home_fav["vegas_spread"].abs() < high)]
        if len(sub) < 10:
            continue
        rows.append({"metric": f"home_fav_spread_{low}_{high}", "value": round(100.0 * sub["covered_vegas"].mean(), 1), "n": len(sub)})

    out_path = _backend_root / "data" / "analysis" / "venue_analysis.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows}


# ---------- Analysis 7: Seed simulation ----------
def analyze_seed_matchups(ats_df: pd.DataFrame, kenpom_df: pd.DataFrame | None) -> dict:
    if kenpom_df is None:
        out_path = _backend_root / "data" / "analysis" / "seed_simulation.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return {}

    ats = ats_df.copy()
    ats["home_rank"] = ats["home_team"].map(lambda t: _get_team_rank(t, kenpom_df))
    ats["away_rank"] = ats["away_team"].map(lambda t: _get_team_rank(t, kenpom_df))

    def rank_to_seed(r: int) -> str:
        if r <= 4: return "1"
        if r <= 10: return "2"
        if r <= 16: return "3"
        if r <= 24: return "4"
        if r <= 36: return "5-6"
        if r <= 52: return "7-8"
        if r <= 68: return "9-10"
        if r <= 100: return "11-12"
        return "13-16"

    ats["home_seed"] = ats["home_rank"].map(rank_to_seed)
    ats["away_seed"] = ats["away_rank"].map(rank_to_seed)

    rows = []
    # 1v16 type (1-4 vs 100+)
    one_sixteen = ats[(ats["home_rank"] <= 4) & (ats["away_rank"] > 100)] if "home_rank" in ats.columns else pd.DataFrame()
    one_sixteen = pd.concat([one_sixteen, ats[(ats["away_rank"] <= 4) & (ats["home_rank"] > 100)]])
    if len(one_sixteen) >= 5:
        mae = np.abs(one_sixteen["actual_margin_home"] - one_sixteen["kenpom_predicted_margin"]).mean()
        kc = one_sixteen["covered_kenpom"].sum()
        tot = one_sixteen["covered_kenpom"].notna().sum()
        rows.append({"matchup": "1v16_type", "n": len(one_sixteen), "kenpom_mae": round(float(mae), 2), "kenpom_cover_pct": round(100.0 * kc / tot, 1) if tot else None})

    # 5v12, 6v11, 7v10
    for h, a in [(5, 12), (6, 11), (7, 10)]:
        hr, ar = (h, 11) if h == 5 else ((6, 11) if h == 6 else (7, 10))
        r_lo, r_hi = (5, 10) if h == 5 else ((11, 20) if h == 6 else (21, 35))
        a_lo, a_hi = (37, 52) if h == 5 else ((53, 68) if h == 6 else (37, 52))
        sub = ats[((ats["home_rank"] >= r_lo) & (ats["home_rank"] <= r_hi) & (ats["away_rank"] >= a_lo) & (ats["away_rank"] <= a_hi)) |
               ((ats["away_rank"] >= r_lo) & (ats["away_rank"] <= r_hi) & (ats["home_rank"] >= a_lo) & (ats["home_rank"] <= a_hi))]
        if len(sub) >= 5:
            mae = np.abs(sub["actual_margin_home"] - sub["kenpom_predicted_margin"]).mean()
            upset = (sub["home_rank"] > sub["away_rank"]) & (sub["actual_margin_home"] < 0) | (sub["away_rank"] > sub["home_rank"]) & (sub["actual_margin_home"] > 0)
            rows.append({"matchup": f"{h}v{a}_type", "n": len(sub), "kenpom_mae": round(float(mae), 2), "upset_rate_pct": round(100.0 * upset.sum() / len(sub), 1)})

    out_path = _backend_root / "data" / "analysis" / "seed_simulation.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return {"rows": rows}


def _round_dict(obj, decimals=2):
    if isinstance(obj, dict):
        return {k: _round_dict(v, decimals) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_round_dict(x, decimals) for x in obj]
    if isinstance(obj, float):
        return round(obj, decimals) if not (obj != obj) else None
    return obj


def print_executive_summary(results: dict) -> None:
    print("\n" + "=" * 60)
    print("BRACKETIQ MODEL ANALYSIS — EXECUTIVE SUMMARY")
    print("=" * 60)

    conf = results.get("conference", {})
    if conf.get("rows"):
        rows = conf["rows"]
        best = rows[0] if rows else {}
        worst = rows[-1] if len(rows) > 1 else {}
        print("\n• Best-predicted conference (lowest MAE):", best.get("conference"), f"(MAE {best.get('kenpom_mae')})")
        print("• Worst-predicted conference:", worst.get("conference"), f"(MAE {worst.get('kenpom_mae')})")
        big_edge = max(rows, key=lambda r: r.get("kp_edge_over_vegas_when_disagree_pct") or 0)
        print("• Conference where KenPom has biggest edge over Vegas (when they disagree):", big_edge.get("conference"), f"({big_edge.get('kp_edge_over_vegas_when_disagree_pct')}%)")

    tot = results.get("totals", {})
    if tot.get("rows"):
        actionable = [r for r in tot["rows"] if r.get("actionable")]
        if actionable:
            print("\n• Strongest totals correlation (actionable):", actionable[0].get("metric"), "r =", actionable[0].get("pearson_r"))
        else:
            print("\n• No totals correlation with |r| > 0.15 (actionable).")

    edge = results.get("edge", {})
    if edge.get("rows"):
        thresh_rows = [r for r in edge["rows"] if "|edge|>" in str(r.get("edge_bucket", ""))]
        best_thresh = None
        for r in thresh_rows:
            pct = r.get("kenpom_cover_rate_pct")
            if pct and pct > 52 and r.get("n", 0) >= 30:
                best_thresh = r
                break
        if best_thresh:
            print("\n• Optimal KenPom edge threshold (profitable signal):", best_thresh.get("edge_bucket"), f"(cover rate {best_thresh.get('kenpom_cover_rate_pct')}%, n={best_thresh.get('n')})")
        else:
            print("\n• No edge threshold with cover rate > 52% and n>=30.")

    up = results.get("upsets", {})
    if up.get("rows"):
        print("\n• Total upsets in dataset:", up.get("n_upsets", 0))
    print("\n• Recency weighting vs pure KenPom: (run with recency data for last 3 weeks to validate.)")
    print("=" * 60)


def print_validation_checklist(ats_df: pd.DataFrame, results: dict) -> None:
    """Print validation checklist with actual values (expected ranges from audit prompt)."""
    print("\n" + "=" * 60)
    print("VALIDATION CHECKLIST")
    print("=" * 60)
    v = ats_df

    def flag(val, lo, hi):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return "N/A"
        ok = (lo <= val <= hi) if (lo is not None and hi is not None) else True
        s = f"{val:.2f}" if isinstance(val, (int, float)) else str(val)
        return s + (" ✓" if ok else " RED FLAG")

    spread_vals = v["vegas_spread"].dropna()
    spread_min = float(spread_vals.min()) if len(spread_vals) else None
    spread_max = float(spread_vals.max()) if len(spread_vals) else None
    spread_ok = len(spread_vals) and (spread_vals != 0).any() and spread_min is not None and spread_max is not None
    spread_mean = float(spread_vals.mean()) if len(spread_vals) else None
    mean_margin = v["actual_margin_home"].mean()
    correct_pick = (np.sign(v["kenpom_predicted_margin"]) == np.sign(v["actual_margin_home"])).mean() * 100
    kp_cover = v["covered_kenpom"].dropna()
    kp_cover_pct = kp_cover.mean() * 100 if len(kp_cover) else None
    veg_cover = v["covered_vegas"].dropna()
    veg_cover_pct = veg_cover.mean() * 100 if len(veg_cover) else None
    mae = (v["actual_margin_home"] - v["kenpom_predicted_margin"]).abs().mean()
    edge_series = v["kenpom_vs_vegas_edge"].dropna()
    edge_mean = edge_series.mean() if len(edge_series) else None
    edge_pos_count = (v["kenpom_vs_vegas_edge"] > 0).sum()
    edge_neg_count = (v["kenpom_vs_vegas_edge"] < 0).sum()
    n_edge = edge_pos_count + edge_neg_count
    edge_pos_pct = 100.0 * edge_pos_count / n_edge if n_edge else 0
    edge_neg_pct = 100.0 * edge_neg_count / n_edge if n_edge else 0
    sp_rows = len(results.get("spread_profiles", {}).get("rows", []))
    upset_rows = results.get("upsets", {}).get("rows", [])
    upset_sections = len(upset_rows)  # sections/buckets in upset analysis

    print(f"  vegas_spread range:          [{spread_min:.2f} to {spread_max:.2f}]  (expect -30 to +30, both signs) {'✓' if spread_ok else 'RED FLAG'}")
    print(f"  vegas_spread mean:            {(f'{spread_mean:.2f}' if spread_mean is not None else 'N/A')}  (expect -2.0 to +2.0) {flag(spread_mean, -2.0, 2.0)}")
    print(f"  actual_margin_home mean:     {mean_margin:.2f}  (expect negative, dataset bias OK) {'✓' if mean_margin is not None and mean_margin < 2 else ' note'}")
    print(f"  covered_vegas True %:        {veg_cover_pct:.1f}%  (expect 47-53%) {flag(veg_cover_pct, 47, 53)}")
    print(f"  covered_kenpom True %:       {kp_cover_pct:.1f}%  (expect 47-53%) {flag(kp_cover_pct, 47, 53)}")
    print(f"  KenPom correct pick %:       {correct_pick:.1f}%  (expect 70-78%) {flag(correct_pick, 70, 78)}")
    print(f"  KenPom MAE:                  {mae:.2f}  (expect 8.5-10.5) {flag(mae, 8.5, 10.5)}")
    print(f"  kenpom_vs_vegas_edge mean:   {(f'{edge_mean:.2f}' if edge_mean is not None else 'N/A')}  (expect -1.5 to +1.5) {flag(edge_mean, -1.5, 1.5)}")
    print(f"  edge positive count:         {edge_pos_count}  (expect >30% of games) {'✓' if n_edge and edge_pos_pct > 30 else 'RED FLAG'}")
    print(f"  edge negative count:         {edge_neg_count}  (expect >30% of games) {'✓' if n_edge and edge_neg_pct > 30 else 'RED FLAG'}")
    print(f"  spread_profiles rows:        {sp_rows}  (expect 15+) {'✓' if sp_rows >= 15 else 'RED FLAG'}")
    print(f"  upset analysis sections:     {upset_sections}  (expect 6) {'✓' if upset_sections >= 6 else ' note'}")
    if "home_away_aligned" in v.columns:
        aligned = v["home_away_aligned"].sum()
        flipped = (~v["home_away_aligned"]).sum()
        print(f"  home/away aligned count:     {int(aligned)}")
        print(f"  home/away flipped count:     {int(flipped)}")
    print("=" * 60)


def main() -> int:
    print("Running model analysis...")
    try:
        ats_df = load_ats_complete()
    except FileNotFoundError as e:
        print(e)
        return 1
    print(f"  Loaded {len(ats_df)} ATS games.")

    kenpom_df = load_kenpom_ratings()
    ff_df = load_four_factors()
    ts_off, ts_def = load_teamstats()
    ht_df = load_height()
    if kenpom_df is None:
        print("  KenPom cache: not found (some analyses limited).")
    else:
        print(f"  KenPom cache: loaded ({len(kenpom_df)} teams).")

    results = {}
    print("  Conference accuracy...")
    results["conference"] = analyze_conference_accuracy(ats_df, kenpom_df)
    print("  Totals correlation...")
    results["totals"] = analyze_totals_correlation(ats_df, kenpom_df)
    print("  Spread by profile...")
    results["spread_profiles"] = analyze_spread_by_profile(ats_df, kenpom_df)
    print("  KenPom vs Vegas edge...")
    results["edge"] = analyze_kenpom_vs_vegas_edge(ats_df)
    print("  Upset patterns...")
    results["upsets"] = analyze_upset_patterns(ats_df, kenpom_df)
    print("  Venue effects...")
    results["venue"] = analyze_venue_effects(ats_df, kenpom_df)
    print("  Seed simulation...")
    results["seeds"] = analyze_seed_matchups(ats_df, kenpom_df)

    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "analysis_report.json"
    # JSON-serializable: remove non-serializable, round floats
    def to_serializable(obj):
        if isinstance(obj, dict):
            return {k: to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [to_serializable(x) for x in obj]
        if isinstance(obj, (np.integer, np.floating)):
            return float(obj) if not (obj != obj) else None
        if isinstance(obj, np.bool_):
            return bool(obj)
        return obj
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(to_serializable(results), f, indent=2)
    print(f"Saved report to {report_path}")

    print_executive_summary(results)
    print_validation_checklist(ats_df, results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
