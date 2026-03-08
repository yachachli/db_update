"""
BracketIQ — Audit raw odds parquet to diagnose 24% cover rate.
Step 1: Check distribution of consensus_spread in odds_2026.parquet (before alignment).
Run from backend: py -m scripts.audit_odds_extraction
"""
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend))

import pandas as pd

def main():
    for base in [_backend / "data" / "historical", _backend / "app" / "data" / "historical"]:
        path = base / "odds_2026.parquet"
        if not path.exists():
            continue
        odds = pd.read_parquet(path)
        print("=== odds_2026.parquet ===")
        print(f"Path: {path}")
        print(f"Columns: {odds.columns.tolist()}")
        print(f"\nShape: {odds.shape}")
        print(f"\nconsensus_spread describe:")
        print(odds["consensus_spread"].describe())

        # KEY CHECK: distribution of spread signs in RAW odds (before any alignment)
        raw_pos = (odds["consensus_spread"] > 0).sum()
        raw_neg = (odds["consensus_spread"] < 0).sum()
        raw_zero = (odds["consensus_spread"] == 0).sum()
        print(f"\nRaw spread > 0 (Odds API home team is underdog): {raw_pos}")
        print(f"Raw spread < 0 (Odds API home team is favorite): {raw_neg}")
        print(f"Raw spread == 0: {raw_zero}")
        print("\n  In a normal odds dataset, home team is favorite slightly more often (HCA).")
        print("  If raw spreads are overwhelmingly positive, we may be extracting the AWAY team's spread.")

        print(f"\nSample rows (first 10):")
        print(odds.head(10).to_string())
        return 0
    print("odds_2026.parquet not found in data/historical or app/data/historical")
    return 1


if __name__ == "__main__":
    sys.exit(main())
