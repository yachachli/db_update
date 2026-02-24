"""
VORP refresh pipeline — self-contained module for GitHub Actions.

Reads game stats from nba_player_game_stats (READ-ONLY), estimates BPM
using pre-trained v2b coefficients, calculates VORP, and upserts to
the player_season_vorp table in Neon.
"""

import datetime as _dt
import json
import os
import re

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MODULE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(MODULE_DIR, "data")
REPLACEMENT_LEVEL = -2.0
FULL_SEASON_GAMES = 82
MODEL_VERSION = "v2b"

# =====================================================================
# DATABASE CONNECTION
# =====================================================================

def get_engine():
    """Build a SQLAlchemy engine from environment variables.

    Supports two modes:
      1. DATABASE_URL env var (used locally with .env)
      2. Individual DB_USER / DB_PASS / DB_HOST / DB_NAME
         (used in GitHub Actions via repo secrets)
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("DB_USER")
        pw = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        name = os.getenv("DB_NAME")
        if not all([user, pw, host, name]):
            raise RuntimeError(
                "Set DATABASE_URL or DB_USER/DB_PASS/DB_HOST/DB_NAME"
            )
        url = f"postgresql://{user}:{pw}@{host}/{name}?sslmode=require"
    return create_engine(url)


# =====================================================================
# SEASON HELPER
# =====================================================================

def _game_date_to_season(d):
    if d is None:
        return None
    if isinstance(d, str):
        d = _dt.date.fromisoformat(d)
    return f"{d.year}-{str(d.year + 1)[-2:]}" if d.month >= 10 \
        else f"{d.year - 1}-{str(d.year)[-2:]}"


# =====================================================================
# NAME NORMALIZATION
# =====================================================================

_NAME_ALIASES = {
    "nic claxton": "nicolas claxton",
    "cam thomas": "cameron thomas",
    "bub carrington": "carlton carrington",
    "gg jackson": "gregory jackson",
    "dominick barlow": "dom barlow",
}


def _normalize_name(name):
    s = name.strip().lower()
    s = re.sub(r"\bjr\.?\b", "", s)
    s = re.sub(r"\bsr\.?\b", "", s)
    s = re.sub(r"\biii\b", "", s)
    s = re.sub(r"\bii\b", "", s)
    s = re.sub(r"\biv\b", "", s)
    s = s.replace(".", "").replace("'", "").replace("\u2019", "")
    s = re.sub(r"\s+", " ", s).strip()
    return _NAME_ALIASES.get(s, s)


# =====================================================================
# LOAD MODEL COEFFICIENTS
# =====================================================================

def _load_coefficients():
    path = os.path.join(DATA_DIR, "bpm_coefficients.json")
    with open(path) as f:
        blob = json.load(f)
    return blob["coefficients"], blob["league_averages"]


# =====================================================================
# PULL GAME DATA FROM NEON (READ-ONLY)
# =====================================================================

def _load_game_data(engine, season_filter=None):
    print("  Pulling data from Neon (read-only)...")
    with engine.connect() as conn:
        games = pd.read_sql(text("SELECT * FROM nba_player_game_stats"), conn)
        players = pd.read_sql(
            text("SELECT player_id, name, position FROM nba_players"), conn
        )

    df = games.merge(
        players.rename(columns={"name": "player_name"}),
        on="player_id", how="left",
    )
    df["season"] = df["game_date"].apply(_game_date_to_season)

    if season_filter:
        df = df[df["season"] == season_filter].copy()

    df = df[df["minutes_played"].notna() & (df["minutes_played"] > 0)].copy()

    stat_cols = [
        "points", "rebounds", "assists", "steals", "blocks", "turnovers",
        "offensive_rebounds", "defensive_rebounds", "field_goal_attempts",
        "field_goals_made", "free_throw_attempts", "free_throws_made",
        "three_point_fg_made", "three_point_fg_attempts", "personal_fouls",
        "plus_minus",
    ]
    df[stat_cols] = df[stat_cols].fillna(0)

    tg = df.groupby(["team_abv", "game_id"]).agg(
        team_fga=("field_goal_attempts", "sum"),
        team_fta=("free_throw_attempts", "sum"),
        team_tov=("turnovers", "sum"),
        team_orb=("offensive_rebounds", "sum"),
        team_min=("minutes_played", "sum"),
    ).reset_index()
    tg["team_poss"] = (tg["team_fga"] + 0.44 * tg["team_fta"]
                       + tg["team_tov"] - tg["team_orb"]).clip(lower=1)
    tg["team_min"] = tg["team_min"].clip(lower=1)

    df = df.merge(tg[["team_abv", "game_id", "team_poss", "team_min"]],
                  on=["team_abv", "game_id"], how="left")

    rate = (20 * df["team_min"]) / (df["team_poss"] * df["minutes_played"])
    for raw, r100 in {
        "points": "pts_100", "field_goal_attempts": "fga_100",
        "field_goals_made": "fgm_100", "free_throw_attempts": "fta_100",
        "free_throws_made": "ftm_100", "three_point_fg_made": "tpm_100",
        "three_point_fg_attempts": "tpa_100",
        "offensive_rebounds": "orb_100", "defensive_rebounds": "drb_100",
        "assists": "ast_100", "steals": "stl_100", "blocks": "blk_100",
        "turnovers": "tov_100", "personal_fouls": "pf_100",
    }.items():
        df[r100] = df[raw] * rate

    print(f"  {len(df):,} game rows for {season_filter or 'all seasons'}")
    return df


# =====================================================================
# BPM MODEL
# =====================================================================

def _compute_team_net_rtg(df):
    tg_pm = df.groupby(["team_abv", "game_id", "season"]).agg(
        sum_pm=("plus_minus", "sum"),
        t_poss=("team_poss", "first"),
    ).reset_index()
    tg_pm["net_rtg"] = (tg_pm["sum_pm"] / 5) * 100 / tg_pm["t_poss"]
    return tg_pm.groupby(["team_abv", "season"])["net_rtg"].mean().reset_index(
        name="team_net_rtg"
    )


def _apply_bpm(df, coefficients, lg_avgs):
    lg_tsa = lg_avgs["fga_100"] + 0.44 * lg_avgs["fta_100"]
    lg_ts_pct = lg_avgs["pts_100"] / (2 * lg_tsa) if lg_tsa > 0 else 0.56

    p_tsa = df["fga_100"] + 0.44 * df["fta_100"]
    expected_pts = 2 * p_tsa * lg_ts_pct

    feat = {
        "scoring_eff": df["pts_100"] - expected_pts,
        "volume_dev": p_tsa - lg_tsa,
        "ast_dev": df["ast_100"] - lg_avgs["ast_100"],
        "orb_dev": df["orb_100"] - lg_avgs["orb_100"],
        "drb_dev": df["drb_100"] - lg_avgs["drb_100"],
        "stl_dev": df["stl_100"] - lg_avgs["stl_100"],
        "blk_dev": df["blk_100"] - lg_avgs["blk_100"],
        "tov_dev": df["tov_100"] - lg_avgs["tov_100"],
        "pf_dev": df["pf_100"] - lg_avgs["pf_100"],
        "tpm_dev": df["tpm_100"] - lg_avgs["tpm_100"],
    }

    is_big = ((df["position"] == "C") | (df["position"] == "PF")).astype(float)
    feat["ast_x_big"] = feat["ast_dev"] * is_big
    feat["scoring_x_vol"] = feat["scoring_eff"] * feat["volume_dev"].clip(lower=0)

    bpm = np.full(len(df), coefficients["intercept"])
    for key, series in feat.items():
        if key in coefficients:
            vals = series.values if hasattr(series, "values") else series
            bpm += coefficients[key] * vals

    for pos in ["PG", "SG", "PF", "C", "G"]:
        col = f"pos_{pos}"
        if col in coefficients:
            bpm[df["position"] == pos] += coefficients[col]

    df["game_bpm_raw"] = bpm

    team_rtg = _compute_team_net_rtg(df)
    df = df.merge(team_rtg, on=["team_abv", "season"], how="left")
    df["team_net_rtg"] = df["team_net_rtg"].fillna(0)
    df["game_bpm"] = df["game_bpm_raw"] + \
        coefficients.get("team_net_rtg", 0) * df["team_net_rtg"]

    return df


# =====================================================================
# AGGREGATION + VORP
# =====================================================================

def _aggregate(df):
    ps = df.groupby(["player_id", "player_name", "team_abv", "season"]).apply(
        lambda g: pd.Series({
            "games_played": len(g),
            "total_minutes": g["minutes_played"].sum(),
            "avg_bpm": np.average(g["game_bpm"], weights=g["minutes_played"]),
        }),
        include_groups=False,
    ).reset_index()

    ts = df.groupby(["team_abv", "season"]).agg(
        team_games=("game_id", "nunique"),
        team_total_minutes=("minutes_played", "sum"),
    ).reset_index()

    return ps, ts


def _calculate_vorp(ps, ts):
    df = ps.merge(ts, on=["team_abv", "season"], how="left")
    df["pct_minutes"] = df["total_minutes"] / (df["team_total_minutes"] / 5)
    df["pct_minutes"] = df["pct_minutes"].clip(upper=1.0)
    df["vorp"] = (
        (df["avg_bpm"] - REPLACEMENT_LEVEL)
        * df["pct_minutes"]
        * (df["team_games"] / FULL_SEASON_GAMES)
    )
    df.loc[df["avg_bpm"].isna(), "vorp"] = np.nan
    df["calculated_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
    return df


# =====================================================================
# DB WRITE (UPSERT)
# =====================================================================

_CREATE_TABLE_SQL = text("""
CREATE TABLE IF NOT EXISTS player_season_vorp (
    id SERIAL PRIMARY KEY,
    player_id VARCHAR(50),
    player_name VARCHAR(100),
    team VARCHAR(10),
    season VARCHAR(10),
    games_played INTEGER,
    total_minutes NUMERIC(8,1),
    pct_minutes NUMERIC(5,4),
    team_games INTEGER,
    estimated_bpm NUMERIC(6,2),
    vorp NUMERIC(6,2),
    model_version VARCHAR(10) DEFAULT 'v2b',
    calculated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, team, season)
);
""")

_INDEX_SQLS = [
    text("CREATE INDEX IF NOT EXISTS idx_vorp_season ON player_season_vorp(season);"),
    text("CREATE INDEX IF NOT EXISTS idx_vorp_team_season ON player_season_vorp(team, season);"),
    text("CREATE INDEX IF NOT EXISTS idx_vorp_player ON player_season_vorp(player_id);"),
    text("CREATE INDEX IF NOT EXISTS idx_vorp_value ON player_season_vorp(season, vorp DESC);"),
]

_UPSERT_SQL = text("""
    INSERT INTO player_season_vorp
        (player_id, player_name, team, season, games_played,
         total_minutes, pct_minutes, team_games, estimated_bpm,
         vorp, model_version, calculated_at)
    VALUES
        (:player_id, :player_name, :team, :season, :games_played,
         :total_minutes, :pct_minutes, :team_games, :estimated_bpm,
         :vorp, :model_version, NOW())
    ON CONFLICT (player_id, team, season) DO UPDATE SET
        player_name = EXCLUDED.player_name,
        games_played = EXCLUDED.games_played,
        total_minutes = EXCLUDED.total_minutes,
        pct_minutes = EXCLUDED.pct_minutes,
        team_games = EXCLUDED.team_games,
        estimated_bpm = EXCLUDED.estimated_bpm,
        vorp = EXCLUDED.vorp,
        model_version = EXCLUDED.model_version,
        calculated_at = NOW()
""")


def _write_to_neon(df, engine, model_version=MODEL_VERSION):
    with engine.begin() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        for idx_sql in _INDEX_SQLS:
            conn.execute(idx_sql)

    rename_map = {"team_abv": "team", "avg_bpm": "estimated_bpm"}
    df = df.rename(columns=rename_map)
    df["player_id"] = df["player_id"].astype(str)

    keep = [
        "player_id", "player_name", "team", "season", "games_played",
        "total_minutes", "pct_minutes", "team_games", "estimated_bpm", "vorp",
    ]
    records = df[keep].to_dict("records")
    for rec in records:
        rec["model_version"] = model_version
        rec["games_played"] = int(rec["games_played"])
        rec["team_games"] = int(rec["team_games"])
        rec["total_minutes"] = round(float(rec["total_minutes"]), 1)
        rec["pct_minutes"] = round(float(rec["pct_minutes"]), 4)
        rec["estimated_bpm"] = round(float(rec["estimated_bpm"]), 2)
        rec["vorp"] = round(float(rec["vorp"]), 2)

    with engine.begin() as conn:
        conn.execute(_UPSERT_SQL, records)

    print(f"  Upserted {len(records)} rows (model: {model_version})")
    return len(records)


# =====================================================================
# PUBLIC API
# =====================================================================

def refresh_vorp(engine, season=None):
    """Full pipeline: read -> BPM -> VORP -> write. Returns the VORP DataFrame."""
    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%H:%M:%S UTC")
    print(f"[{ts}] VORP Refresh starting for season: {season or 'all'}")

    coefficients, lg_avgs = _load_coefficients()
    print(f"  Model: {MODEL_VERSION} ({len(coefficients)} coefficients)")

    df = _load_game_data(engine, season_filter=season)
    df = _apply_bpm(df, coefficients, lg_avgs)

    ps, ts_agg = _aggregate(df)
    print(f"  Aggregated: {len(ps)} player-team-seasons, "
          f"{len(ts_agg)} team-seasons")

    vorp_df = _calculate_vorp(ps, ts_agg)
    print(f"  VORP calculated for {len(vorp_df)} rows")

    _write_to_neon(vorp_df, engine)

    with engine.connect() as conn:
        clause = "WHERE season = :season" if season else ""
        params = {"season": season} if season else {}
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM player_season_vorp {clause}"), params
        ).scalar()

    ts2 = _dt.datetime.now(_dt.timezone.utc).strftime("%H:%M:%S UTC")
    print(f"[{ts2}] VORP Refresh complete. {count} rows for {season or 'all seasons'}.")
    return vorp_df
