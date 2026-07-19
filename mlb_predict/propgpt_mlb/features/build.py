"""Build leakage-safe features for each game from DB tables.

All rolling stats use only games with game_date strictly before the target game.
Designed for walk-forward backtests and live inference on the same code path.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from propgpt_mlb.db import SCHEMA, fetch_all

# Rolling windows (longer = stabler early-season estimates)
SP_WINDOW = 10
TEAM_WINDOW = 20
MIN_SP_STARTS = 3
LEAGUE_AVG_RUNS = 4.5
SEASON_GAMES = 162

# Totals: compact set that beat naive under Ridge (see MODEL_TUNING.md)
TOTALS_FEATURE_COLS = [
    "home_sp_era_adj",
    "away_sp_era_adj",
    "home_runs_vs_sp",
    "away_runs_vs_sp",
    "park_runs_factor",
    "is_dome",
    "season_phase",
    "expected_total",
    "sp_quality_sum",
    "sp_k9_sum",
    "home_sp_k9_adj",
    "away_sp_k9_adj",
    "home_sp_bb9_adj",
    "away_sp_bb9_adj",
    "park_hr_home",
    "park_hr_away",
]

# Moneyline: differential-heavy set for logistic regression
ML_FEATURE_COLS = [
    "sp_era_diff",
    "team_runs_diff",
    "k9_diff",
    "bb9_diff",
    "hr_diff",
    "home_sp_era_adj",
    "away_sp_era_adj",
    "home_runs_vs_sp",
    "away_runs_vs_sp",
    "home_sp_k9_adj",
    "away_sp_k9_adj",
    "park_runs_factor",
    "is_dome",
    "season_phase",
    "home_sp_throws_R",
    "away_sp_throws_R",
    "both_sp_warm",
]

# Union used for feature JSON / inspection
FEATURE_COLS = list(dict.fromkeys(TOTALS_FEATURE_COLS + ML_FEATURE_COLS))

# SP-centric subset for first-five-innings totals
F5_FEATURE_COLS = [
    "home_sp_era_adj",
    "away_sp_era_adj",
    "home_sp_k9_adj",
    "away_sp_k9_adj",
    "home_sp_bb9_adj",
    "away_sp_bb9_adj",
    "home_runs_vs_sp",
    "away_runs_vs_sp",
    "park_runs_factor",
    "is_dome",
    "season_phase",
    "expected_total",
    "sp_quality_sum",
    "sp_era_diff",
    "team_runs_diff",
    "k9_diff",
]


def _ip_to_float(ip: Any) -> float:
    if ip is None:
        return 0.0
    try:
        v = float(ip)
        whole = int(v)
        thirds = round((v - whole) * 10)
        return whole + thirds / 3.0
    except (TypeError, ValueError):
        return 0.0


def load_training_games(
    *,
    season: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Games with outcomes and labels, ordered chronologically."""
    clauses = ["o.game_id IS NOT NULL"]
    params: list[Any] = []
    if season is not None:
        clauses.append("g.season = %s")
        params.append(season)
    if start_date:
        clauses.append("g.game_date >= %s")
        params.append(start_date)
    if end_date:
        clauses.append("g.game_date <= %s")
        params.append(end_date)

    where = " AND ".join(clauses)
    rows = fetch_all(
        f"""
        SELECT
            g.game_id,
            g.game_date,
            g.season,
            g.home_team_id,
            g.away_team_id,
            g.park_id,
            g.home_sp_id,
            g.away_sp_id,
            ht.abbr AS home_abbr,
            at.abbr AS away_abbr,
            p.is_dome,
            hp.throws AS home_sp_throws,
            ap.throws AS away_sp_throws,
            o.home_score,
            o.away_score,
            o.total_runs,
            o.home_won,
            o.home_runs_f5,
            o.away_runs_f5,
            o.total_runs_f5
        FROM {SCHEMA}.games g
        JOIN {SCHEMA}.outcomes o ON g.game_id = o.game_id
        JOIN {SCHEMA}.teams ht ON g.home_team_id = ht.team_id
        JOIN {SCHEMA}.teams at ON g.away_team_id = at.team_id
        LEFT JOIN {SCHEMA}.parks p ON g.park_id = p.park_id
        LEFT JOIN {SCHEMA}.players hp ON g.home_sp_id = hp.player_id
        LEFT JOIN {SCHEMA}.players ap ON g.away_sp_id = ap.player_id
        WHERE {where}
          AND g.home_sp_id IS NOT NULL
          AND g.away_sp_id IS NOT NULL
        ORDER BY g.game_date, g.game_time_utc NULLS LAST, g.game_id
        """,
        tuple(params) if params else None,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def load_pitcher_logs() -> pd.DataFrame:
    rows = fetch_all(
        f"""
        SELECT pgl.player_id, pgl.game_id, pgl.team_id, pgl.is_starter,
               pgl.ip, pgl.er, pgl.k, pgl.bb, pgl.hr, pgl.h,
               g.game_date, g.season, g.home_team_id, g.away_team_id
        FROM {SCHEMA}.pitcher_game_logs pgl
        JOIN {SCHEMA}.games g ON pgl.game_id = g.game_id
        WHERE pgl.is_starter = TRUE
        ORDER BY g.game_date, pgl.game_id
        """
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["ip_dec"] = df["ip"].map(_ip_to_float)
    df["opp_team_id"] = df.apply(
        lambda r: r.away_team_id if r.team_id == r.home_team_id else r.home_team_id,
        axis=1,
    )
    return df


def load_team_logs() -> pd.DataFrame:
    rows = fetch_all(
        f"""
        SELECT tgl.team_id, tgl.game_id, tgl.is_home, tgl.runs_scored, tgl.hr,
               tgl.opp_starter_throws,
               g.game_date, g.season
        FROM {SCHEMA}.team_game_logs tgl
        JOIN {SCHEMA}.games g ON tgl.game_id = g.game_id
        ORDER BY g.game_date, tgl.game_id
        """
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def load_park_factors() -> pd.DataFrame:
    rows = fetch_all(
        f"""
        SELECT park_id, season, runs_factor, hr_factor,
               hr_factor_lhb, hr_factor_rhb
        FROM {SCHEMA}.park_factors
        """
    )
    return pd.DataFrame(rows)


def _default_sp_stats() -> dict[str, float]:
    return {"era_adj": 9.0, "k9_adj": 6.0, "bb9_adj": 3.0, "ip_avg": 5.0, "starts": 0.0}


def _default_team_stats() -> dict[str, float]:
    return {"runs_vs_sp": LEAGUE_AVG_RUNS, "hr_vs_sp": 1.0}


def _team_runs_pg(tgl: pd.DataFrame, team_id: int, before: pd.Timestamp) -> float:
    hist = tgl[(tgl["team_id"] == team_id) & (tgl["game_date"] < before)].tail(TEAM_WINDOW)
    if hist.empty:
        return LEAGUE_AVG_RUNS
    return float(hist["runs_scored"].fillna(0).mean())


def _team_platoon_rolling(
    tgl: pd.DataFrame,
    team_id: int,
    before: pd.Timestamp,
    opp_throws: str | None,
) -> dict[str, float]:
    """Team offense vs opposing starter handedness (falls back to all starts)."""
    hist = tgl[(tgl["team_id"] == team_id) & (tgl["game_date"] < before)]
    if opp_throws:
        platoon = hist[hist["opp_starter_throws"] == opp_throws]
        if len(platoon) >= 5:
            hist = platoon
    hist = hist.tail(TEAM_WINDOW)
    if hist.empty:
        return _default_team_stats()
    return {
        "runs_vs_sp": float(hist["runs_scored"].fillna(0).mean()),
        "hr_vs_sp": float(hist["hr"].fillna(0).mean()),
    }


def _sp_rolling(
    pgl: pd.DataFrame,
    tgl: pd.DataFrame,
    player_id: int,
    before: pd.Timestamp,
) -> dict[str, float]:
    """Opponent-adjusted SP rolling stats over last SP_WINDOW starts."""
    all_hist = pgl[(pgl["player_id"] == player_id) & (pgl["game_date"] < before)]
    starts = float(len(all_hist))
    hist = all_hist.tail(SP_WINDOW)
    if hist.empty:
        return _default_sp_stats()

    adj_er = 0.0
    ip_total = 0.0
    k_total = 0.0
    bb_total = 0.0
    for row in hist.itertuples():
        ip = row.ip_dec
        if ip < 0.1:
            continue
        opp_off = _team_runs_pg(tgl, row.opp_team_id, row.game_date)
        scale = LEAGUE_AVG_RUNS / max(opp_off, 2.0)
        adj_er += float(row.er or 0) * scale
        ip_total += ip
        k_total += float(row.k or 0)
        bb_total += float(row.bb or 0)

    if ip_total < 0.1:
        out = _default_sp_stats()
        out["starts"] = starts
        return out

    return {
        "era_adj": (adj_er / ip_total) * 9.0,
        "k9_adj": (k_total / ip_total) * 9.0,
        "bb9_adj": (bb_total / ip_total) * 9.0,
        "ip_avg": ip_total / len(hist),
        "starts": starts,
    }


def _season_phase(tgl: pd.DataFrame, team_id: int, before: pd.Timestamp) -> float:
    """Fraction of regular season completed (0–1) for a team."""
    n = len(tgl[(tgl["team_id"] == team_id) & (tgl["game_date"] < before)])
    return min(n / SEASON_GAMES, 1.0)


def _park_hr_for_batter_side(park: Any, vs_sp_throws: str | None) -> float:
    """Handedness-aware HR factor: LHB vs RHP, RHB vs LHP."""
    if park is None:
        return 1.0
    lhb = float(park.hr_factor_lhb) if park.hr_factor_lhb else float(park.hr_factor or 1.0)
    rhb = float(park.hr_factor_rhb) if park.hr_factor_rhb else float(park.hr_factor or 1.0)
    if vs_sp_throws == "R":
        return lhb
    if vs_sp_throws == "L":
        return rhb
    return (lhb + rhb) / 2.0


def _add_differential_features(row: dict[str, Any]) -> None:
    """Home-minus-away differentials and derived totals features."""
    row["sp_era_diff"] = row["away_sp_era_adj"] - row["home_sp_era_adj"]
    row["team_runs_diff"] = row["home_runs_vs_sp"] - row["away_runs_vs_sp"]
    row["k9_diff"] = row["home_sp_k9_adj"] - row["away_sp_k9_adj"]
    row["bb9_diff"] = row["away_sp_bb9_adj"] - row["home_sp_bb9_adj"]
    row["hr_diff"] = row["home_hr_vs_sp"] - row["away_hr_vs_sp"]
    row["expected_total"] = (
        (row["home_runs_vs_sp"] + row["away_runs_vs_sp"]) * row["park_runs_factor"]
    )
    row["sp_quality_sum"] = row["home_sp_era_adj"] + row["away_sp_era_adj"]
    row["sp_k9_sum"] = row["home_sp_k9_adj"] + row["away_sp_k9_adj"]
    row["both_sp_warm"] = (
        1.0
        if row["home_sp_starts"] >= MIN_SP_STARTS and row["away_sp_starts"] >= MIN_SP_STARTS
        else 0.0
    )


def build_features_for_games(games: pd.DataFrame) -> pd.DataFrame:
    """Attach FEATURE_COLS to each game row. Chronological order required."""
    if games.empty:
        return games

    pgl = load_pitcher_logs()
    tgl = load_team_logs()
    pf = load_park_factors()
    pf_lookup = {(r.park_id, r.season): r for r in pf.itertuples()}

    rows: list[dict[str, Any]] = []
    for g in games.itertuples():
        gd = g.game_date
        home_sp = _sp_rolling(pgl, tgl, g.home_sp_id, gd)
        away_sp = _sp_rolling(pgl, tgl, g.away_sp_id, gd)
        home_tm = _team_platoon_rolling(tgl, g.home_team_id, gd, g.away_sp_throws)
        away_tm = _team_platoon_rolling(tgl, g.away_team_id, gd, g.home_sp_throws)

        pk = (g.park_id, g.season)
        park = pf_lookup.get(pk)
        park_runs = float(park.runs_factor) if park is not None and park.runs_factor else 1.0
        park_hr_home = _park_hr_for_batter_side(park, g.away_sp_throws)
        park_hr_away = _park_hr_for_batter_side(park, g.home_sp_throws)

        row: dict[str, Any] = {
            "game_id": g.game_id,
            "game_date": gd,
            "home_abbr": g.home_abbr,
            "away_abbr": g.away_abbr,
            "home_sp_era_adj": home_sp["era_adj"],
            "away_sp_era_adj": away_sp["era_adj"],
            "home_sp_k9_adj": home_sp["k9_adj"],
            "away_sp_k9_adj": away_sp["k9_adj"],
            "home_sp_bb9_adj": home_sp["bb9_adj"],
            "away_sp_bb9_adj": away_sp["bb9_adj"],
            "home_sp_ip_avg": home_sp["ip_avg"],
            "away_sp_ip_avg": away_sp["ip_avg"],
            "home_sp_starts": home_sp["starts"],
            "away_sp_starts": away_sp["starts"],
            "home_runs_vs_sp": home_tm["runs_vs_sp"],
            "away_runs_vs_sp": away_tm["runs_vs_sp"],
            "home_hr_vs_sp": home_tm["hr_vs_sp"],
            "away_hr_vs_sp": away_tm["hr_vs_sp"],
            "park_runs_factor": park_runs,
            "park_hr_home": park_hr_home,
            "park_hr_away": park_hr_away,
            "is_dome": 1.0 if g.is_dome else 0.0,
            "home_sp_throws_R": 1.0 if g.home_sp_throws == "R" else 0.0,
            "away_sp_throws_R": 1.0 if g.away_sp_throws == "R" else 0.0,
            "season_phase": _season_phase(tgl, g.home_team_id, gd),
            "total_runs": getattr(g, "total_runs", None),
            "home_won": int(g.home_won) if getattr(g, "home_won", None) is not None else None,
            "home_runs_f5": getattr(g, "home_runs_f5", None),
            "away_runs_f5": getattr(g, "away_runs_f5", None),
            "total_runs_f5": getattr(g, "total_runs_f5", None),
        }
        _add_differential_features(row)
        rows.append(row)

    return pd.DataFrame(rows)


def features_json(row: pd.Series) -> dict[str, float]:
    return {c: float(row[c]) for c in FEATURE_COLS if c in row.index and pd.notna(row[c])}
