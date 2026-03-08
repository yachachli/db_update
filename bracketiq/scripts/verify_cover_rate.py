"""
BracketIQ — Verify covered_vegas is in 47–53% after odds/ATS pipeline.
Run from backend: py -m scripts.verify_cover_rate [--diagnose]
Do not run model_analysis until this passes.
"""
import argparse
import sys
from pathlib import Path

_backend = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend))

import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--diagnose", action="store_true", help="Print alternative formula rates to find sign/convention bug")
    args = parser.parse_args()
    path = _backend / "data" / "historical" / "ats_complete_2026.parquet"
    if not path.exists():
        path = _backend / "app" / "data" / "historical" / "ats_complete_2026.parquet"
    if not path.exists():
        print("ats_complete_2026.parquet not found. Run build_ats_dataset first.")
        return 1
    df = pd.read_parquet(path)
    rate = df["covered_vegas"].mean()
    home_win_rate = (df["actual_margin_home"] > 0).mean()
    same = df[df["home_away_aligned"] == True]["covered_vegas"].mean() if "home_away_aligned" in df.columns else None
    flipped = df[df["home_away_aligned"] == False]["covered_vegas"].mean() if "home_away_aligned" in df.columns else None
    print(f"home_win_rate (actual_margin_home > 0): {home_win_rate:.3f} ({home_win_rate*100:.1f}%)  (expect 58-62%)")
    print(f"covered_vegas rate: {rate:.3f} ({rate*100:.1f}%)  (expect 47-53%)")
    if same is not None:
        print(f"Same alignment cover: {same:.3f}")
    if flipped is not None:
        print(f"Flipped alignment cover: {flipped:.3f}")
    if args.diagnose:
        # Standard: cover when (actual_margin_home + vegas_spread) > 0 (Option A: spread = home team points)
        alt = (df["actual_margin_home"] - df["vegas_spread"]) > 0
        inv = (df["actual_margin_home"] + df["vegas_spread"]) <= 0  # inverted outcome
        # If margin sign is wrong (we stored away margin): use -margin
        neg_margin = (-df["actual_margin_home"] + df["vegas_spread"]) > 0
        print("\n--- Diagnostic (which formula gives ~50%?) ---")
        print(f"  (margin + spread) > 0:       {df['covered_vegas'].mean():.3f}")
        print(f"  (margin - spread) > 0:       {alt.mean():.3f}")
        print(f"  (margin + spread) <= 0:     {inv.mean():.3f} (inverted)")
        print(f"  (-margin + spread) > 0:     {neg_margin.mean():.3f} (negated margin)")
        if 0.47 <= neg_margin.mean() <= 0.53:
            print("  -> (-margin + spread) > 0 is ~50%: actual_margin_home may be stored from away POV (fix in build_ats_dataset).")
        if not any(0.47 <= x <= 0.53 for x in [rate, alt.mean(), inv.mean(), neg_margin.mean()]):
            print("  -> No simple sign flip gives ~50%. Re-run collect_historical_odds then build_ats_dataset; spot-check margin+spread vs reality.")
    if 0.47 <= rate <= 0.53:
        print("PASS — Ready for analysis")
        return 0
    print("FAIL — Still broken, need more debugging")
    return 1


if __name__ == "__main__":
    sys.exit(main())
