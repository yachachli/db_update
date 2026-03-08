"""Quick check: covered_vegas rate by home_away_aligned (same vs flipped). Run from backend: py -m scripts.check_ats_alignment"""
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend))

import pandas as pd

path = _backend / "data" / "historical" / "ats_complete_2026.parquet"
if not path.exists():
    path = _backend / "app" / "data" / "historical" / "ats_complete_2026.parquet"
if not path.exists():
    print("ats_complete_2026.parquet not found")
    sys.exit(1)

df = pd.read_parquet(path)
if "home_away_aligned" in df.columns:
    print(df["home_away_aligned"].value_counts())
    flipped = df[df["home_away_aligned"] == False]["covered_vegas"].mean()
    same = df[df["home_away_aligned"] == True]["covered_vegas"].mean()
    print(f"\nFlipped games covered_vegas rate: {flipped:.3f}")
    print(f"Same games covered_vegas rate: {same:.3f}")
else:
    print("home_away_aligned column not found")
print(f"\nOverall covered_vegas rate: {df['covered_vegas'].mean():.3f}")
print("\nVegas spread describe:")
print(df["vegas_spread"].describe())
