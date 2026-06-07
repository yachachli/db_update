"""Remove player_ratings_history rows for non-WC squad team_codes.

Inspect-only by default; pass --apply to DELETE stale bootstrap rows.

    py -3 scripts/clean_ratings_history.py
    py -3 scripts/clean_ratings_history.py --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import DatabaseError, get_connection, get_wc2026_squad_team_codes  # noqa: E402


def _history_stats() -> dict[str, int]:
    with get_connection() as conn:
        total = conn.execute(
            "SELECT COUNT(*) AS n FROM player_ratings_history"
        ).fetchone()
        entities = conn.execute(
            "SELECT COUNT(DISTINCT entity_key) AS n FROM player_ratings_history"
        ).fetchone()
        teams = conn.execute(
            "SELECT COUNT(DISTINCT team_code) AS n FROM player_ratings_history"
        ).fetchone()
    return {
        "rows": int(total["n"]) if total else 0,
        "entity_groups": int(entities["n"]) if entities else 0,
        "team_codes": int(teams["n"]) if teams else 0,
    }


def _stale_team_report(allowed: set[str]) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT team_code,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT entity_key) AS entity_groups
            FROM player_ratings_history
            WHERE team_code <> ALL(%s)
            GROUP BY team_code
            ORDER BY team_code ASC
            """,
            (list(allowed),),
        ).fetchall()
        samples: dict[str, list[str]] = {}
        for row in rows:
            code = str(row["team_code"])
            sample_rows = conn.execute(
                """
                SELECT entity_key
                FROM player_ratings_history
                WHERE team_code = %s
                ORDER BY entity_key ASC
                LIMIT 3
                """,
                (code,),
            ).fetchall()
            samples[code] = [str(r["entity_key"]) for r in sample_rows]
    return [
        {
            "team_code": str(row["team_code"]),
            "row_count": int(row["row_count"]),
            "entity_groups": int(row["entity_groups"]),
            "sample_entity_keys": samples.get(str(row["team_code"]), []),
        }
        for row in rows
    ]


def _delete_stale(allowed: set[str]) -> int:
    with get_connection() as conn:
        with conn.transaction():
            result = conn.execute(
                """
                DELETE FROM player_ratings_history
                WHERE team_code <> ALL(%s)
                """,
                (list(allowed),),
            )
    return int(result.rowcount or 0)


def _current_team_codes() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT team_code
            FROM player_ratings_current
            ORDER BY team_code ASC
            """
        ).fetchall()
    return [str(r["team_code"]) for r in rows]


def inspect() -> int:
    allowed = set(get_wc2026_squad_team_codes())
    before = _history_stats()
    stale = _stale_team_report(allowed)
    current_codes = _current_team_codes()
    orphan_current = sorted(set(current_codes) - allowed)

    print("=" * 78)
    print("CLEAN player_ratings_history — INSPECT ONLY")
    print("=" * 78)
    print(f"Allowed WC squad codes: {len(allowed)}")
    print(f"\nBefore: rows={before['rows']}  entity_groups={before['entity_groups']}  "
          f"distinct team_codes={before['team_codes']}")

    if not stale:
        print("\nNo stale team_codes outside the 48 squad list.")
    else:
        print(f"\nStale team_codes ({len(stale)}):")
        for row in stale:
            samples = ", ".join(row["sample_entity_keys"]) or "(none)"
            print(
                f"  {row['team_code']:<6} rows={row['row_count']:<5} "
                f"entities={row['entity_groups']:<4} samples: {samples}"
            )

    print(f"\nplayer_ratings_current distinct team_codes: {len(current_codes)}")
    if orphan_current:
        print(f"  OUTSIDE 48 squad list: {orphan_current}")

    print("\nSTOP — inspect only. Re-run with --apply to delete stale rows.")
    return 0


def apply() -> int:
    allowed = set(get_wc2026_squad_team_codes())
    before = _history_stats()
    stale = _stale_team_report(allowed)

    print("=" * 78)
    print("CLEAN player_ratings_history — APPLY")
    print("=" * 78)
    print(f"Before: rows={before['rows']}  entity_groups={before['entity_groups']}  "
          f"team_codes={before['team_codes']}")

    if stale:
        print(f"\nDeleting {sum(r['row_count'] for r in stale)} rows across "
              f"{len(stale)} stale team_codes...")
        deleted = _delete_stale(allowed)
    else:
        print("\nNo stale rows to delete.")
        deleted = 0

    after = _history_stats()
    current_codes = _current_team_codes()
    orphan_current = sorted(set(current_codes) - allowed)

    print(f"\nDeleted rows: {deleted}")
    print(f"After:  rows={after['rows']}  entity_groups={after['entity_groups']}  "
          f"team_codes={after['team_codes']}")
    print(f"\nplayer_ratings_current distinct team_codes: {len(current_codes)}")
    if orphan_current:
        print(f"  WARNING — still outside 48: {orphan_current}")
    else:
        print("  OK — all current entity groups are within the 48 squad codes.")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Delete stale rows")
    args = parser.parse_args()
    try:
        return apply() if args.apply else inspect()
    except DatabaseError as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
