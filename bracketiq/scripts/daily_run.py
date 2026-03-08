"""
BracketIQ — Daily run: one day of games with KenPom vs Vegas edge, sorted by edge (biggest first).
Use as presentation example. Saves JSON and CSV to data/analysis/.
Usage: py -m scripts.daily_run [--date YYYY-MM-DD]
  Default: use the most recent date in ats_complete_2026.parquet.
"""
import argparse
import json
import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

import pandas as pd
import numpy as np


def _load_ats():
    for base in [_backend_root / "data" / "historical", _backend_root / "app" / "data" / "historical"]:
        path = base / "ats_complete_2026.parquet"
        if path.exists():
            return pd.read_parquet(path)
    return None


def main():
    parser = argparse.ArgumentParser(description="Daily run: games by date, ordered by KenPom vs Vegas edge")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: latest date in dataset)")
    args = parser.parse_args()

    df = _load_ats()
    if df is None or df.empty:
        print("ats_complete_2026.parquet not found or empty. Run build_ats_dataset first.")
        return 1

    if args.date:
        date_str = args.date[:10]
        day = df[df["game_date"].astype(str).str[:10] == date_str]
        if day.empty:
            print(f"No games for {date_str}. Using latest date in dataset instead.")
            date_str = str(df["game_date"].max())[:10]
            day = df[df["game_date"].astype(str).str[:10] == date_str]
    else:
        date_str = str(df["game_date"].max())[:10]
        day = df[df["game_date"].astype(str).str[:10] == date_str]

    if day.empty:
        print("No games found.")
        return 1

    # Sort by absolute edge descending (biggest model-vs-Vegas disagreement first)
    day = day.copy()
    day["edge_abs"] = day["kenpom_vs_vegas_edge"].abs().fillna(0)
    day = day.sort_values("edge_abs", ascending=False).drop(columns=["edge_abs"])

    # Build output rows for JSON/CSV (presentation-friendly)
    rows = []
    for _, r in day.iterrows():
        edge = r.get("kenpom_vs_vegas_edge")
        rows.append({
            "game_date": str(r["game_date"])[:10],
            "away_team": r["away_team"],
            "home_team": r["home_team"],
            "kenpom_predicted_margin_home_pov": round(float(r["kenpom_predicted_margin"]), 2) if pd.notna(r.get("kenpom_predicted_margin")) else None,
            "vegas_spread_home_pov": round(float(r["vegas_spread"]), 2) if pd.notna(r.get("vegas_spread")) else None,
            "edge": round(float(edge), 2) if pd.notna(edge) else None,
            "edge_interpretation": "Model likes HOME more than Vegas" if pd.notna(edge) and float(edge) > 0 else ("Model likes AWAY more than Vegas" if pd.notna(edge) and float(edge) < 0 else "—"),
            "actual_margin_home": round(float(r["actual_margin_home"]), 1) if pd.notna(r.get("actual_margin_home")) else None,
            "covered_vegas": bool(r["covered_vegas"]) if pd.notna(r.get("covered_vegas")) else None,
            "vegas_total": round(float(r["vegas_total"]), 1) if pd.notna(r.get("vegas_total")) else None,
            "actual_total": round(float(r["actual_total"]), 1) if pd.notna(r.get("actual_total")) else None,
            "home_rank": int(r["home_rank"]) if pd.notna(r.get("home_rank")) and r["home_rank"] is not None else None,
            "away_rank": int(r["away_rank"]) if pd.notna(r.get("away_rank")) and r["away_rank"] is not None else None,
        })

    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_date = date_str.replace("-", "")
    json_path = out_dir / f"daily_run_{safe_date}.json"
    csv_path = out_dir / f"daily_run_{safe_date}.csv"

    payload = {
        "date": date_str,
        "total_games": len(rows),
        "games_ordered_by_edge": "descending (biggest KenPom vs Vegas edge first)",
        "games": rows,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    pd.DataFrame(rows).to_csv(csv_path, index=False)
    print(f"Daily run for {date_str} ({len(rows)} games, ordered by edge)")
    print(f"  JSON: {json_path}")
    print(f"  CSV:  {csv_path}")
    print("\nTop 5 by |edge| (KenPom vs Vegas):")
    for i, row in enumerate(rows[:5], 1):
        e = row.get("edge")
        interp = row.get("edge_interpretation", "")
        print(f"  {i}. {row['away_team']} @ {row['home_team']}  edge={e}  ({interp})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
