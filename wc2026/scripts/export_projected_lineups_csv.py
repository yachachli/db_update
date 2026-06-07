"""Export current projected lineups from Neon to a CSV file.

Reads the ``projected_lineups_csv`` view (latest snapshot per team/slot).

Run from the project root:

    python scripts/export_projected_lineups_csv.py
    python scripts/export_projected_lineups_csv.py --output reports/projected_lineups.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseError, export_projected_lineups_csv_rows  # noqa: E402

_CSV_FIELDS = (
    "team_code",
    "team_name",
    "snapshot_date",
    "lineup_role",
    "lineup_slot",
    "squad_no",
    "player_name",
    "position",
    "avg_rating",
    "minutes_share",
    "matches_counted",
    "match_method",
    "team_xi_status",
    "ratings_source",
    "computed_at",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("reports/projected_lineups.csv"),
        help="Output CSV path (default: reports/projected_lineups.csv)",
    )
    args = parser.parse_args()

    try:
        rows = export_projected_lineups_csv_rows()
    except DatabaseError as exc:
        print(f"ERROR: could not read projected lineups from Neon: {exc}")
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            out = dict(row)
            if out.get("snapshot_date") is not None:
                out["snapshot_date"] = str(out["snapshot_date"])
            if out.get("computed_at") is not None:
                out["computed_at"] = str(out["computed_at"])
            writer.writerow(out)

    xi_rows = sum(1 for row in rows if row.get("lineup_role") == "projected_xi")
    teams = len({row["team_code"] for row in rows})
    print(f"Wrote {len(rows)} rows ({xi_rows} XI slots) for {teams} teams -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
