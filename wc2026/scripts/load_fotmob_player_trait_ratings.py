"""Load data/fotmob_player_trait_ratings.csv into Neon."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.database import DatabaseError, upsert_fotmob_player_trait_ratings  # noqa: E402

CSV_PATH = ROOT / "data" / "fotmob_player_trait_ratings.csv"


def _parse_bool(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _parse_int(value: str | None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(float(text))


def main() -> int:
    if not CSV_PATH.exists():
        print(f"ERROR: missing {CSV_PATH}")
        return 1

    with CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict] = []
        for raw in reader:
            rows.append(
                {
                    "team": raw["team"].strip(),
                    "group": raw["group"].strip(),
                    "player_name": raw.get("player_name") or None,
                    "player_rank_used": int(raw["player_rank_used"]),
                    "fotmob_id": _parse_int(raw.get("fotmob_id")),
                    "fotmob_url": raw.get("fotmob_url") or None,
                    "compared_to": raw.get("compared_to") or None,
                    "has_traits": _parse_bool(raw.get("has_traits")),
                    "trait1_name": raw.get("trait1_name") or None,
                    "trait1_pct": _parse_int(raw.get("trait1_pct")),
                    "trait2_name": raw.get("trait2_name") or None,
                    "trait2_pct": _parse_int(raw.get("trait2_pct")),
                    "trait3_name": raw.get("trait3_name") or None,
                    "trait3_pct": _parse_int(raw.get("trait3_pct")),
                    "trait4_name": raw.get("trait4_name") or None,
                    "trait4_pct": _parse_int(raw.get("trait4_pct")),
                    "trait5_name": raw.get("trait5_name") or None,
                    "trait5_pct": _parse_int(raw.get("trait5_pct")),
                    "trait6_name": raw.get("trait6_name") or None,
                    "trait6_pct": _parse_int(raw.get("trait6_pct")),
                    "traits_json": raw.get("traits_json") or None,
                    "scraped_at": raw.get("scraped_at") or None,
                    "data_source": raw.get("data_source") or "fotmob_playerData",
                }
            )

    print("=" * 70)
    print("LOAD FOTMOB PLAYER TRAIT RATINGS -> NEON")
    print("=" * 70)
    print(f"CSV rows: {len(rows)}")
    print(f"With traits: {sum(1 for r in rows if r['has_traits'])}")

    try:
        upsert_fotmob_player_trait_ratings(rows)
    except DatabaseError as exc:
        print(f"ERROR: {exc}")
        return 1

    print(f"Upserted {len(rows)} rows into fotmob_player_trait_ratings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
