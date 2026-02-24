"""
Entry point for the VORP refresh pipeline.

Usage:
    python -m vorp_update              # Refresh all seasons
    python -m vorp_update --season 2025-26   # Single season only
"""

import argparse
import sys
from datetime import datetime, timezone

from .pipeline import refresh_vorp, get_engine


def main():
    parser = argparse.ArgumentParser(description="Refresh player_season_vorp in Neon")
    parser.add_argument(
        "--season", type=str, default=None,
        help="Season to calculate (e.g., 2025-26). Omit for all seasons.",
    )
    args = parser.parse_args()

    try:
        engine = get_engine()
        vorp_df = refresh_vorp(engine, season=args.season)

        qualified = vorp_df[vorp_df["games_played"] >= 20]
        for szn in sorted(qualified["season"].unique()):
            top = qualified[qualified["season"] == szn].nlargest(10, "vorp")
            print(f"\n  TOP 10 VORP -- {szn}:")
            cols = ["player_name", "team_abv", "games_played",
                    "avg_bpm", "pct_minutes", "vorp"]
            print(top[cols].to_string(index=False))

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"\nDone at {ts}")

    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
