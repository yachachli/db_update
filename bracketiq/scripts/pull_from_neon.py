"""
Pull existing BracketIQ historical data from Neon into local parquets.
Run BEFORE incremental collection so scripts know which dates already exist.
Uses bracketiq_* table names (same Neon DB as NBA tables).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))

TABLE_PREFIX = "bracketiq_"


def main() -> int:
    url = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DB_URL")
    if not url:
        print("No NEON_DATABASE_URL / DB_URL — starting fresh (no pull).")
        return 0

    from sqlalchemy import create_engine
    import pandas as pd

    try:
        engine = create_engine(url, connect_args={"sslmode": "require"})
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        return 1

    from app.config import get_historical_dir
    hist = get_historical_dir()
    hist = Path(hist)
    hist.mkdir(parents=True, exist_ok=True)

    # FanMatch — rename back to PascalCase for pipeline
    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_PREFIX}fanmatch_historical", engine)
        if len(df) > 0:
            df = df.drop(columns=["id", "updated_at"], errors="ignore")
            renames = {
                "game": "Game", "predicted_winner": "PredictedWinner", "predicted_loser": "PredictedLoser",
                "predicted_mov": "PredictedMOV", "winner": "Winner", "loser": "Loser",
                "winner_score": "WinnerScore", "loser_score": "LoserScore", "actual_mov": "ActualMOV",
                "location": "Location", "thrill_score": "ThrillScore",
            }
            df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})
            df.to_parquet(hist / "fanmatch_2026.parquet", index=False)
            print(f"  Pulled fanmatch: {len(df)} rows")
    except Exception as e:
        print(f"  fanmatch: {e}")

    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_PREFIX}odds_historical", engine)
        if len(df) > 0:
            df = df.drop(columns=["id", "updated_at"], errors="ignore")
            df.to_parquet(hist / "odds_2026.parquet", index=False)
            print(f"  Pulled odds: {len(df)} rows")
    except Exception as e:
        print(f"  odds: {e}")

    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_PREFIX}ats_historical", engine)
        if len(df) > 0:
            df = df.drop(columns=["id", "updated_at"], errors="ignore")
            df.to_parquet(hist / "ats_complete_2026.parquet", index=False)
            print(f"  Pulled ats: {len(df)} rows")
    except Exception as e:
        print(f"  ats: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
