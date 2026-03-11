"""
Push BracketIQ data to Neon. Creates tables automatically (if_exists="replace").
Use from backend/ or from db_update/bracketiq/ with working-directory set to this folder's parent.

Tables (prefixed with bracketiq_ to avoid conflicts with NBA tables in same DB):
  - bracketiq_kenpom_ratings, bracketiq_kenpom_fourfactors, bracketiq_kenpom_teamstats_off/def, bracketiq_kenpom_height
  - bracketiq_slate_today, bracketiq_ats_historical, bracketiq_fanmatch_historical, bracketiq_odds_historical

Usage:
  python -m scripts.push_to_neon              # push all
  python -m scripts.push_to_neon --only kenpom   # only KenPom tables (for manual workflow)
  python -m scripts.push_to_neon --only slate    # only slate_today
  python -m scripts.push_to_neon --only historical  # only ats, fanmatch, odds
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

# Load .env (same as app.config) so NEON_DATABASE_URL/DB_URL are set when run from bracketiq
import app.config  # noqa: E402

TABLE_PREFIX = "bracketiq_"
VALID_ONLY = ("kenpom", "slate", "historical", "all")


def _engine():
    url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DB_URL")
    if not url:
        raise ValueError("NEON_DATABASE_URL or DB_URL not set")
    from sqlalchemy import create_engine
    return create_engine(url, connect_args={"sslmode": "require"})


def _cache_dir() -> Path:
    from app.config import get_cache_dir
    return get_cache_dir()


def _hist_dir() -> Path:
    from app.config import get_historical_dir
    return get_historical_dir()


def _latest_parquet(cache_dir: Path, pattern: str):
    import pandas as pd
    files = list(cache_dir.glob(pattern))
    if not files:
        return None
    return pd.read_parquet(max(files, key=lambda p: p.stat().st_mtime))


def _clean(df):
    if df is None or len(df) == 0:
        return df
    df = df.copy()
    df.columns = [
        str(c).lower().replace(" ", "_").replace(".", "_").replace("-", "_")
        for c in df.columns
    ]
    drop = [c for c in df.columns if c.endswith("_rank") and c != "rank"]
    return df.drop(columns=drop, errors="ignore")


def _push(engine, name: str, df, add_updated_at: bool = True):
    import pandas as pd
    if df is None or len(df) == 0:
        print(f"  SKIP {TABLE_PREFIX}{name} (no data)")
        return
    table = f"{TABLE_PREFIX}{name}"
    df = _clean(df)
    if add_updated_at:
        df["updated_at"] = pd.Timestamp.now()
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"  OK   {table}: {len(df)} rows")


def _push_kenpom_ratings(engine):
    import pandas as pd
    cache = _cache_dir()
    df = _latest_parquet(cache, "pomeroy_ratings_*.parquet")
    if df is None or len(df) == 0:
        print(f"  SKIP {TABLE_PREFIX}kenpom_ratings (no data)")
        return
    cols = {"Team": "team", "Conf": "conference", "Rk": "rank", "AdjO": "adj_oe", "AdjD": "adj_de",
            "AdjEM": "adj_em", "AdjT": "adj_tempo", "Luck": "luck"}
    df = df.rename(columns={k: v for k, v in cols.items() if k in df.columns})
    want = ["team", "conference", "rank", "adj_oe", "adj_de", "adj_em", "adj_tempo", "luck"]
    opt = {"SOS Adj EM": "sos_adj_em", "SOS Adj OE": "sos_adj_oe", "SOS Adj DE": "sos_adj_de", "NCSOS Adj EM": "ncsos_adj_em"}
    for old, new in opt.items():
        if old in df.columns:
            want.append(new)
            df = df.rename(columns={old: new})
    df = df[[c for c in want if c in df.columns]]
    df["updated_at"] = pd.Timestamp.now()
    df.to_sql(f"{TABLE_PREFIX}kenpom_ratings", engine, if_exists="replace", index=False)
    print(f"  OK   {TABLE_PREFIX}kenpom_ratings: {len(df)} rows")


def _push_slate(engine):
    import pandas as pd
    for sub in ["data/analysis", "app/data/analysis"]:
        path = _BASE / sub / "slate_today.json"
        if path.exists():
            break
    else:
        print(f"  SKIP {TABLE_PREFIX}slate_today (file not found)")
        return
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    games = data.get("all_games", [])
    if not games:
        print(f"  SKIP {TABLE_PREFIX}slate_today (no games)")
        return
    df = pd.DataFrame(games)
    # Serialize nested dict columns so PostgreSQL can store them (e.g. markets)
    for col in df.columns:
        if df[col].dtype == object and df[col].notna().any():
            sample = df[col].dropna().iloc[0]
            if isinstance(sample, dict):
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    _push(engine, "slate_today", df)


def _push_fanmatch(engine):
    import pandas as pd
    path = _hist_dir() / "fanmatch_2026.parquet"
    if not path.exists():
        print(f"  SKIP {TABLE_PREFIX}fanmatch_historical (not found)")
        return
    df = pd.read_parquet(path)
    keep = ["fanmatch_date", "Game", "PredictedWinner", "PredictedLoser", "PredictedMOV", "PredictedScore",
            "Winner", "Loser", "WinnerScore", "LoserScore", "ActualMOV", "Location", "ThrillScore"]
    df = df[[c for c in keep if c in df.columns]]
    renames = {"Game": "game", "PredictedWinner": "predicted_winner", "PredictedLoser": "predicted_loser",
               "PredictedMOV": "predicted_mov", "PredictedScore": "predicted_score", "Winner": "winner", "Loser": "loser",
               "WinnerScore": "winner_score", "LoserScore": "loser_score", "ActualMOV": "actual_mov",
               "Location": "location", "ThrillScore": "thrill_score"}
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})
    _push(engine, "fanmatch_historical", df)


def main() -> int:
    parser = argparse.ArgumentParser(description="Push BracketIQ data to Neon (optionally only certain table groups)")
    parser.add_argument(
        "--only",
        choices=VALID_ONLY,
        default="all",
        help="Push only this group: kenpom (5 tables), slate (1), historical (3), or all (default)",
    )
    args = parser.parse_args()
    only = args.only

    try:
        engine = _engine()
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    if only in ("all", "kenpom"):
        print("=== KenPom ===")
        _push_kenpom_ratings(engine)
        _push(engine, "kenpom_fourfactors", _latest_parquet(_cache_dir(), "fourfactors_*.parquet"))
        _push(engine, "kenpom_teamstats_off", _latest_parquet(_cache_dir(), "teamstats_off_*.parquet"))
        _push(engine, "kenpom_teamstats_def", _latest_parquet(_cache_dir(), "teamstats_def_*.parquet"))
        _push(engine, "kenpom_height", _latest_parquet(_cache_dir(), "height_*.parquet"))

    if only in ("all", "slate"):
        print("\n=== Slate ===")
        _push_slate(engine)

    if only in ("all", "historical"):
        print("\n=== Historical ===")
        ats = _hist_dir() / "ats_complete_2026.parquet"
        _push(engine, "ats_historical", __import__("pandas").read_parquet(ats) if ats.exists() else None)
        _push_fanmatch(engine)
        odds = _hist_dir() / "odds_2026.parquet"
        _push(engine, "odds_historical", __import__("pandas").read_parquet(odds) if odds.exists() else None)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
