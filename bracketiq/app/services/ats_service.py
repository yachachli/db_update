"""
Task 5 / Phase 1.5: ATS Performance by NET Tier.
Uses ats_complete_2026.parquet when available (FanMatch + Vegas odds); else FanMatch-only.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import get_historical_dir


def _net_tier(rank: int) -> int:
    if rank <= 10:
        return 1
    if rank <= 25:
        return 2
    if rank <= 50:
        return 3
    if rank <= 75:
        return 4
    return 5


def _record_from_bools(covers: int, total: int) -> dict:
    if total == 0:
        return {"record": "0-0", "cover_pct": 0.0}
    return {"record": f"{covers}-{total - covers}", "cover_pct": round(100.0 * covers / total, 1)}


def _load_ats_complete() -> Optional[pd.DataFrame]:
    hist = get_historical_dir()
    path = hist / "ats_complete_2026.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def _team_in_game(team: str, home: str, away: str) -> bool:
    t = str(team).strip().lower()
    return t == home.strip().lower() or t == away.strip().lower() or t in home.strip().lower() or t in away.strip().lower()


def get_ats_by_team(team_name: str, season: Optional[str] = None) -> dict:
    """
    ATS record: vs Vegas and vs KenPom, by NET tier, as favorite/underdog, O/U.
    Uses ats_complete_2026.parquet when available.
    """
    team_name = str(team_name).strip()
    ac = _load_ats_complete()
    if ac is not None and len(ac) > 0:
        return _get_ats_from_complete(team_name, ac)
    return _get_ats_from_fanmatch_only(team_name)


def _get_ats_from_complete(team_name: str, df: pd.DataFrame) -> dict:
    team_games = []
    for _, row in df.iterrows():
        home, away = row.get("home_team"), row.get("away_team")
        if not _team_in_game(team_name, str(home or ""), str(away or "")):
            continue
        is_home = str(home).strip().lower() == team_name.strip().lower() or (team_name.strip().lower() in str(home).strip().lower())
        margin = row.get("actual_margin_home")
        if not is_home:
            margin = -float(margin) if margin is not None else None
        vegas_spread = row.get("vegas_spread")
        kp_mov = row.get("kenpom_predicted_margin")
        team_games.append({
            **row.to_dict(),
            "is_home": is_home,
            "team_margin": margin,
            "opponent_rank": int(row.get("away_rank") or 99) if is_home else int(row.get("home_rank") or 99),
            "covered_vegas": row.get("covered_vegas"),
            "covered_kenpom": row.get("covered_kenpom"),
            "was_favorite_vegas": (vegas_spread is not None and float(vegas_spread) < 0) if is_home else (vegas_spread is not None and float(vegas_spread) > 0),
            "over_under": row.get("over_under_result"),
        })
    if not team_games:
        return _empty_ats_full_structure(team_name)
    g = pd.DataFrame(team_games)
    vs_vegas_c = g["covered_vegas"].sum()
    vs_vegas_t = g["covered_vegas"].notna().sum()
    vs_kenpom_c = g["covered_kenpom"].sum()
    vs_kenpom_t = g["covered_kenpom"].notna().sum()
    fav = g[g["was_favorite_vegas"]]
    dog = g[~g["was_favorite_vegas"]]
    edges = g["kenpom_vs_vegas_edge"].dropna()
    over_under = g["over_under"].value_counts()
    by_tier = {}
    for t in range(1, 6):
        tier_g = g[g["opponent_rank"].apply(lambda r: _net_tier(r) == t)]
        vt = tier_g["covered_vegas"].notna().sum()
        vc = tier_g["covered_vegas"].sum()
        kt = tier_g["covered_kenpom"].notna().sum()
        kc = tier_g["covered_kenpom"].sum()
        by_tier[f"tier_{t}"] = {
            "vs_vegas": _record_from_bools(int(vc), int(vt)) if vt else {"record": "0-0", "cover_pct": 0.0},
            "vs_kenpom": _record_from_bools(int(kc), int(kt)) if kt else {"record": "0-0", "cover_pct": 0.0},
        }
    return {
        "team": team_name,
        "overall": {
            "vs_vegas": _record_from_bools(int(vs_vegas_c), int(vs_vegas_t)),
            "vs_kenpom": _record_from_bools(int(vs_kenpom_c), int(vs_kenpom_t)),
        },
        "by_net_tier": by_tier,
        "as_favorite": {
            "vs_vegas": _record_from_bools(int(fav["covered_vegas"].sum()), int(fav["covered_vegas"].notna().sum())),
            "vs_kenpom": _record_from_bools(int(fav["covered_kenpom"].sum()), int(fav["covered_kenpom"].notna().sum())),
        } if len(fav) else {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "as_underdog": {
            "vs_vegas": _record_from_bools(int(dog["covered_vegas"].sum()), int(dog["covered_vegas"].notna().sum())),
            "vs_kenpom": _record_from_bools(int(dog["covered_kenpom"].sum()), int(dog["covered_kenpom"].notna().sum())),
        } if len(dog) else {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "over_under": {"over": int(over_under.get("over", 0)), "under": int(over_under.get("under", 0)), "push": int(over_under.get("push", 0))},
        "kenpom_edge_analysis": {
            "avg_edge_vs_vegas": round(float(edges.mean()), 1) if len(edges) else None,
            "edge_cover_rate": round(float(g["covered_kenpom"].sum() / g["covered_kenpom"].notna().sum()), 2) if g["covered_kenpom"].notna().any() else None,
        },
    }


def _empty_ats_full_structure(team_name: str) -> dict:
    return {
        "team": team_name,
        "overall": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "by_net_tier": {f"tier_{t}": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}} for t in range(1, 6)},
        "as_favorite": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "as_underdog": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "over_under": {"over": 0, "under": 0, "push": 0},
        "kenpom_edge_analysis": {"avg_edge_vs_vegas": None, "edge_cover_rate": None},
    }


def _get_ats_from_fanmatch_only(team_name: str) -> dict:
    """Fallback when ats_complete_2026.parquet not available."""
    hist = get_historical_dir()
    path = hist / "fanmatch_2026.parquet"
    if not path.exists():
        return _empty_ats_full_structure(team_name)
    df = pd.read_parquet(path)
    mask = pd.Series(False, index=df.index)
    for col in ["Winner", "Loser", "PredictedWinner"]:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.contains(team_name, na=False, case=False)
    team_games = df[mask]
    if team_games.empty:
        return _empty_ats_full_structure(team_name)
    covers = total = 0
    for _, row in team_games.iterrows():
        if pd.isna(row.get("ActualMOV")) or pd.isna(row.get("PredictedMOV")):
            continue
        total += 1
        am = float(row["ActualMOV"])
        pm = float(row["PredictedMOV"])
        winner = str(row.get("Winner", ""))
        if team_name.lower() in winner.lower():
            if am >= pm - 0.5:
                covers += 1
        else:
            if -am >= -pm - 0.5:
                covers += 1
    return {
        "team": team_name,
        "overall": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": _record_from_bools(covers, total)},
        "by_net_tier": {f"tier_{t}": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}} for t in range(1, 6)},
        "as_favorite": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "as_underdog": {"vs_vegas": {"record": "0-0", "cover_pct": 0.0}, "vs_kenpom": {"record": "0-0", "cover_pct": 0.0}},
        "over_under": {"over": 0, "under": 0, "push": 0},
        "kenpom_edge_analysis": {"avg_edge_vs_vegas": None, "edge_cover_rate": None},
        "message": "Vegas data not loaded. Run collect_historical_odds and build_ats_dataset.",
    }


def _empty_ats_structure() -> dict:
    return {
        "ats_by_tier": {str(t): {"record": "0-0", "cover_pct": 0.0, "avg_margin_vs_spread": None} for t in range(1, 6)},
        "overall": {"record": "0-0", "cover_pct": 0.0, "avg_margin_vs_spread": None},
    }


def get_ats_performance_breakdown(season: Optional[str] = None) -> dict:
    ac = _load_ats_complete()
    if ac is not None and len(ac) > 0:
        by_tier = {}
        for t in range(1, 6):
            # Use home_rank/away_rank for tier
            g = ac[(ac["home_rank"].apply(lambda r: _net_tier(int(r) if r is not None else 99) == t) | (ac["away_rank"].apply(lambda r: _net_tier(int(r) if r is not None else 99) == t))]
            vt = g["covered_vegas"].notna().sum()
            vc = g["covered_vegas"].sum()
            by_tier[str(t)] = {"games": int(vt), "covers": int(vc), "cover_pct": round(100.0 * vc / vt, 1) if vt else 0.0}
        return {"by_tier": by_tier, "by_conference": {}, "message": "From ats_complete_2026.parquet (FanMatch + Vegas)."}
    hist = get_historical_dir()
    if not (hist / "fanmatch_2026.parquet").exists():
        return {"by_tier": {}, "by_conference": {}, "message": "No historical data."}
    df = pd.read_parquet(hist / "fanmatch_2026.parquet")
    by_tier = {str(t): {"games": 0, "covers": 0, "cover_pct": 0.0} for t in range(1, 6)}
    valid = df.dropna(subset=["ActualMOV", "PredictedMOV"])
    for _, row in valid.iterrows():
        tier = 5
        by_tier[str(tier)]["games"] += 1
        if abs(float(row["ActualMOV"]) - float(row["PredictedMOV"])) < 0.5:
            by_tier[str(tier)]["covers"] += 1
    for t, v in by_tier.items():
        if v["games"] > 0:
            v["cover_pct"] = round(100.0 * v["covers"] / v["games"], 1)
    return {"by_tier": by_tier, "by_conference": {}, "message": "FanMatch only (no Vegas)."}


def get_conference_accuracy(conference: str, season: Optional[str] = None) -> dict:
    hist = get_historical_dir()
    fm_path = hist / "fanmatch_2026.parquet"
    if not fm_path.exists():
        return {"conference": conference, "games_analyzed": 0, "kenpom_mae": None, "kenpom_favorite_record": None, "ats_vs_vegas": None, "best_predicted_tier": None, "worst_predicted_tier": None}
    df = pd.read_parquet(fm_path)
    if "Game" not in df.columns:
        return {"conference": conference, "games_analyzed": 0, "kenpom_mae": None, "kenpom_favorite_record": None, "ats_vs_vegas": None, "best_predicted_tier": None, "worst_predicted_tier": None}
    mask = df["Game"].astype(str).str.contains(conference, na=False, case=False)
    conf_df = df[mask].dropna(subset=["ActualMOV", "PredictedMOV"])
    if conf_df.empty:
        return {"conference": conference, "games_analyzed": 0, "kenpom_mae": None, "kenpom_favorite_record": None, "ats_vs_vegas": None, "best_predicted_tier": None, "worst_predicted_tier": None}
    mae = (conf_df["ActualMOV"] - conf_df["PredictedMOV"]).abs().mean()
    fav_covers = (conf_df["ActualMOV"] >= conf_df["PredictedMOV"] - 0.5).sum()
    fav_total = len(conf_df)
    ac = _load_ats_complete()
    ats_vs_vegas = None
    if ac is not None and len(ac) > 0:
        # Filter ac to games involving this conference (home or away in Game string)
        ac_conf = ac[ac["home_team"].astype(str).str.contains(conference, na=False, case=False) | ac["away_team"].astype(str).str.contains(conference, na=False, case=False)]
        if len(ac_conf) > 0:
            cover_rate = ac_conf["covered_vegas"].sum() / ac_conf["covered_vegas"].notna().sum() if ac_conf["covered_vegas"].notna().any() else None
            avg_edge = ac_conf["kenpom_vs_vegas_edge"].mean() if ac_conf["kenpom_vs_vegas_edge"].notna().any() else None
            ats_vs_vegas = {"conference_teams_cover_rate": round(float(cover_rate), 2) if cover_rate is not None else None, "avg_kenpom_edge": round(float(avg_edge), 1) if avg_edge is not None else None}
    return {
        "conference": conference,
        "games_analyzed": len(conf_df),
        "kenpom_mae": round(float(mae), 2),
        "kenpom_favorite_record": f"{int(fav_covers)}-{fav_total - int(fav_covers)}",
        "ats_vs_vegas": ats_vs_vegas,
        "best_predicted_tier": None,
        "worst_predicted_tier": None,
    }
