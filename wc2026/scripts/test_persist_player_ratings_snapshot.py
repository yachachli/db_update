"""Test player_ratings_history persist (Phase 4.1b Part 2).

Bootstraps pool, snapshots all teams once, prints summary. No predictions.

    py -3 scripts/setup_neon_schema.py
    py -3 scripts/test_persist_player_ratings_snapshot.py
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseError, get_player_ratings_snapshot_summary  # noqa: E402
from src.pipeline import bootstrap_tournament_pool  # noqa: E402
from src.player_ratings import snapshot_player_ratings_for_pool  # noqa: E402


def main() -> int:
    print("=" * 78)
    print("TEST: player_ratings_history snapshot persist")
    print("=" * 78)

    try:
        pool = bootstrap_tournament_pool()
        stats = snapshot_player_ratings_for_pool(pool)
        summary = get_player_ratings_snapshot_summary()
    except DatabaseError as exc:
        print(f"ERROR: {exc}")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    print(f"\nPersist stats: {stats}")

    by_source: dict[str, int] = defaultdict(int)
    for row in summary["by_team_source"]:
        by_source[row["source"]] += int(row["n"])

    print(f"\nsnapshot_date: {summary['snapshot_date']}")
    print(f"rows written today: {sum(by_source.values())}")
    print(f"distinct teams: {len({r['team_code'] for r in summary['by_team_source']})}")
    print(f"source breakdown: {dict(by_source)}")

    print("\nplayer_ratings_current sample (ARG top 5):")
    for row in summary["sample_arg"]:
        mins = row.get("minutes_share")
        mins_s = f"{mins:.4f}" if mins is not None else "NULL"
        print(
            f"  {row['team_code']:<4} {row['player_name']:<22} "
            f"avg={row['avg_rating']} n={row['matches_counted']} "
            f"mins_share={mins_s} src={row['source']}"
        )

    print("\nplayer_ratings_current sample (NZL top 5):")
    for row in summary["sample_nzl"]:
        mins = row.get("minutes_share")
        mins_s = f"{mins:.4f}" if mins is not None else "NULL"
        manual = (
            f"sq={row['manual_squad_no']}" if row.get("manual_squad_no") else ""
        )
        print(
            f"  {row['team_code']:<4} {row['player_name']:<22} "
            f"avg={row['avg_rating']} n={row['matches_counted']} "
            f"mins_share={mins_s} src={row['source']} {manual}"
        )

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
