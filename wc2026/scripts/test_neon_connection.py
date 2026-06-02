"""Smoke test for Neon Postgres connectivity and basic reads.

Run from the project root:

    python scripts/test_neon_connection.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseError, get_all_teams, get_connection  # noqa: E402


def main() -> int:
    print("=" * 78)
    print("NEON CONNECTION TEST")
    print("=" * 78)

    try:
        with get_connection() as conn:
            row = conn.execute("SELECT 1 AS ok").fetchone()
            if row is None or row["ok"] != 1:
                print("ERROR: SELECT 1 did not return expected result.")
                return 1
            print("SELECT 1: OK")

            count_row = conn.execute("SELECT COUNT(*) AS n FROM teams").fetchone()
            team_count = int(count_row["n"]) if count_row else 0
            print(f"teams table: {team_count} row(s)")

        if team_count == 0:
            print("\nNOTE: teams table is empty. Run:")
            print("  python scripts/populate_neon_initial.py")

        teams = get_all_teams()
        print(f"\nget_all_teams(): {len(teams)} team(s)")
        if teams:
            print("First 5 teams:")
            for t in teams[:5]:
                print(f"  id={t['team_id']}  {t['name']}  "
                      f"conf={t.get('confederation')}  fifa={t.get('fifa_code')}")
        elif team_count == 0:
            print("  (none)")

    except DatabaseError as exc:
        print(f"\nERROR: {exc}")
        return 1
    except Exception as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("\nConnection test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
