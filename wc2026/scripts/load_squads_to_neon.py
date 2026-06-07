"""Load FIFA WC 2026 squad CSV into Neon (idempotent UPSERT).

Run from the project root:

    py -3 scripts/load_squads_to_neon.py
"""

from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import DatabaseError, upsert_wc2026_squad_rows  # noqa: E402

SQUAD_CSV = _PROJECT_ROOT / "data" / "wc2026_squads.csv"
EXPECTED_TEAMS = 48
EXPECTED_SQUAD_SIZE = 26


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.replace("\x00", "").strip()
    return text or None


def _parse_row(raw: dict[str, str]) -> dict[str, str | int | None]:
    dob = _clean_text(raw.get("dob"))
    height = _clean_text(raw.get("height_cm"))
    return {
        "team_code": _clean_text(raw["team_code"]) or "",
        "team_name": _clean_text(raw["team_name"]) or "",
        "squad_no": int(raw["squad_no"]),
        "position": _clean_text(raw["position"]) or "",
        "player_name": _clean_text(raw["player_name"]) or "",
        "first_names": _clean_text(raw.get("first_names")),
        "last_names": _clean_text(raw.get("last_names")),
        "name_on_shirt": _clean_text(raw.get("name_on_shirt")),
        "dob": dob,
        "club": _clean_text(raw.get("club")),
        "club_country": _clean_text(raw.get("club_country")),
        "height_cm": int(height) if height else None,
    }


def main() -> int:
    if not SQUAD_CSV.exists():
        print(f"ERROR: missing squad CSV at {SQUAD_CSV}")
        return 1

    with SQUAD_CSV.open(encoding="utf-8-sig", newline="") as handle:
        rows = [_parse_row(raw) for raw in csv.DictReader(handle)]

    print("=" * 70)
    print("LOAD WC 2026 SQUADS -> NEON")
    print("=" * 70)
    print(f"CSV rows read: {len(rows)}")

    counts = Counter(str(row["team_code"]) for row in rows)
    print(f"Distinct teams: {len(counts)} (expected {EXPECTED_TEAMS})")

    bad_sizes = {
        code: size for code, size in counts.items() if size != EXPECTED_SQUAD_SIZE
    }
    if bad_sizes:
        print(f"WARNING: unexpected squad sizes: {bad_sizes}")

    try:
        upsert_wc2026_squad_rows(rows)
    except DatabaseError as exc:
        print(f"\nERROR: database write failed: {exc}")
        return 1

    print(f"Upserted {len(rows)} squad rows into wc2026_squads.")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
