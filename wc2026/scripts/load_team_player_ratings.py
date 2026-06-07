"""Load manual per-player rating averages into Neon team_player_ratings.

Reads JSON files from data/manual_player_ratings/*.json, computes minutes-unweighted
averages across fixture_ratings entries, and UPSERTs rows keyed by team_code +
squad_no. Idempotent.

Run from the project root:

    py -3 scripts/load_team_player_ratings.py
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import (  # noqa: E402
    DatabaseError,
    get_wc2026_squad_for_team,
    upsert_team_player_ratings,
)

_MANUAL_DIR = _PROJECT_ROOT / "data" / "manual_player_ratings"


def _shirt_to_squad_no(team_code: str) -> dict[str, int]:
    squad = get_wc2026_squad_for_team(team_code)
    return {
        str(row.get("name_on_shirt", "")).upper(): int(row["squad_no"])
        for row in squad
        if row.get("name_on_shirt")
    }


def _player_name_for_squad(team_code: str, squad_no: int) -> str:
    for row in get_wc2026_squad_for_team(team_code):
        if int(row["squad_no"]) == squad_no:
            return str(row.get("player_name", ""))
    return ""


def _compute_averages(payload: dict) -> list[dict]:
    team_code = str(payload["team_code"])
    shirt_map = _shirt_to_squad_no(team_code)
    accum: dict[int, list[float]] = defaultdict(list)

    for fixture in payload.get("fixture_ratings", []):
        players = fixture.get("players", {})
        if not isinstance(players, dict):
            continue
        for shirt, rating in players.items():
            key = str(shirt).strip()
            if key.isdigit():
                squad_no = int(key)
            elif key.lower().startswith("squad:"):
                squad_no = int(key.split(":", 1)[1])
            else:
                squad_no = shirt_map.get(key.upper())
            if squad_no is None:
                raise ValueError(
                    f"Unknown player key {shirt!r} for team {team_code} "
                    f"in {fixture.get('label')}"
                )
            accum[squad_no].append(float(rating))

    rows: list[dict] = []
    for squad_no, ratings in sorted(accum.items()):
        matches_counted = len(ratings)
        avg_rating = sum(ratings) / matches_counted
        rows.append(
            {
                "squad_no": squad_no,
                "player_name": _player_name_for_squad(team_code, squad_no),
                "avg_rating": avg_rating,
                "matches_counted": matches_counted,
            }
        )
    return rows


def _split_listed_insufficient(
    rows: list[dict],
) -> tuple[list[dict], list[dict]]:
    from src.player_ratings import MIN_RATED_APPEARANCES

    listed: list[dict] = []
    insufficient: list[dict] = []
    for row in rows:
        entry = {
            "player_id": 0,
            "player_name": row["player_name"],
            "avg_rating": round(float(row["avg_rating"]), 2),
            "matches_counted": int(row["matches_counted"]),
            "dob": None,
        }
        if row["matches_counted"] >= MIN_RATED_APPEARANCES:
            listed.append(entry)
        else:
            insufficient.append({**entry, "status": "insufficient_data"})
    listed.sort(key=lambda r: r["avg_rating"], reverse=True)
    insufficient.sort(key=lambda r: (-r["matches_counted"], r["player_name"]))
    return listed, insufficient


def load_file(path: Path) -> tuple[list[dict], list[dict], list[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    team_code = str(payload["team_code"])
    source = str(payload.get("source", "manual"))
    computed = _compute_averages(payload)
    db_rows = [
        {
            "team_code": team_code,
            "squad_no": row["squad_no"],
            "player_name": row["player_name"],
            "avg_rating": row["avg_rating"],
            "matches_counted": row["matches_counted"],
            "source": source,
        }
        for row in computed
    ]
    listed, insufficient = _split_listed_insufficient(computed)
    return db_rows, listed, insufficient


def main() -> int:
    if not _MANUAL_DIR.exists():
        print(f"ERROR: missing {_MANUAL_DIR}")
        return 1

    files = sorted(_MANUAL_DIR.glob("*.json"))
    if not files:
        print(f"No JSON files in {_MANUAL_DIR}")
        return 0

    print("=" * 70)
    print("LOAD team_player_ratings (manual averages)")
    print("=" * 70)

    try:
        for path in files:
            db_rows, listed, insufficient = load_file(path)
            team_code = db_rows[0]["team_code"] if db_rows else path.stem
            upsert_team_player_ratings(db_rows)
            print(f"\n{team_code} ({path.name}):")
            print(f"  rows upserted: {len(db_rows)}")
            print(f"  listed: {len(listed)}")
            print(f"  insufficient_data: {len(insufficient)}")
            if listed:
                print("  top listed:")
                for row in listed[:5]:
                    print(
                        f"    {row['player_name']} "
                        f"avg={row['avg_rating']} n={row['matches_counted']}"
                    )
    except (DatabaseError, ValueError) as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
