"""Phase 4.0b dry-run: match SportMonks rated players to FIFA squad CSV rows.

No DB writes. Loads data/wc2026_squads.csv, samples four WC teams, fetches
recent qualifier fixtures with lineup+player includes, aggregates ratings, and
runs the squad matcher. Exit non-zero only on crash.

Run from the project root:

    py -3 scripts/diag_squad_match.py
"""

from __future__ import annotations

import csv
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src import config  # noqa: E402
from src.player_ratings import (  # noqa: E402
    SquadMatchResult,
    aggregate_player_ratings,
    match_rated_players_to_squad,
    squad_shared_dobs,
)
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("diag_squad_match")

SQUAD_CSV = _PROJECT_ROOT / "data" / "wc2026_squads.csv"

SAMPLE_TEAMS: list[dict[str, Any]] = [
    {"team_code": "USA", "search_name": "United States", "team_id": 18571},
    {"team_code": "KOR", "search_name": "Korea Republic", "team_id": 18567},
    {"team_code": "ARG", "search_name": "Argentina", "team_id": 18644},
    {"team_code": "ESP", "search_name": "Spain", "team_id": 18710},
]

_REPORT_METHODS = (
    "dob+name",
    "dob_only",
    "unmatched_ambiguous",
    "not_in_current_squad",
    "no_dob",
)


def load_squad_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def squad_for_team(rows: list[dict[str, str]], team_code: str) -> list[dict[str, str]]:
    return [row for row in rows if row.get("team_code") == team_code]


def fetch_recent_fixtures_with_lineups(
    client: SportmonksClient, team_id: int, limit: int
) -> list[dict[str, Any]]:
    fixtures_summary = client.get_fixtures_for_team(team_id, limit=limit)
    fixtures: list[dict[str, Any]] = []
    for summary in fixtures_summary:
        fixture_id = summary.get("id")
        if fixture_id is None:
            continue
        fixture = client.get_fixture_with_lineups(int(fixture_id))
        if fixture:
            fixtures.append(fixture)
    return fixtures


def rated_players_from_result(result) -> list[dict[str, Any]]:
    return list(result.listed) + list(result.insufficient_data)


def print_team_report(
    team_code: str,
    search_name: str,
    team_squad: list[dict[str, str]],
    matches: list[SquadMatchResult],
    unmatched_squad: list[dict[str, str]],
) -> dict[str, int]:
    counts = {method: 0 for method in _REPORT_METHODS}
    for match in matches:
        counts[match.method] = counts.get(match.method, 0) + 1

    print("\n" + "=" * 70)
    print(f"TEAM {team_code} ({search_name})")
    print("=" * 70)

    shared = squad_shared_dobs(team_squad)
    if shared:
        print("\nSquad members sharing a DOB (within-team disambiguation test):")
        for dob, rows in sorted(shared.items()):
            names = ", ".join(
                f"#{row.get('squad_no')} {row.get('player_name')}" for row in rows
            )
            print(f"  {dob}: {names}")
    else:
        print("\nSquad members sharing a DOB: none")

    print("\nMethod counts:")
    for method in _REPORT_METHODS:
        print(f"  {method}: {counts.get(method, 0)}")

    flagged = [m for m in matches if m.flagged]
    if flagged:
        print("\ndob_only FLAG rows:")
        for match in flagged:
            print(
                f"  [conf={match.confidence}] SM={match.sportmonks_name!r} "
                f"dob={match.sportmonks_dob} -> squad={match.squad_player_name!r} "
                f"dob={match.squad_dob}"
            )
    else:
        print("\ndob_only FLAG rows: none")

    ambiguous = [m for m in matches if m.method == "unmatched_ambiguous"]
    if ambiguous:
        print("\nMulti-DOB / ambiguous ties:")
        for match in ambiguous:
            print(
                f"  id={match.sportmonks_player_id} name={match.sportmonks_name!r} "
                f"dob={match.sportmonks_dob}"
            )
    else:
        print("\nMulti-DOB / ambiguous ties: none")

    not_in_squad = [m for m in matches if m.method == "not_in_current_squad"]
    if not_in_squad:
        print("\nnot_in_current_squad (former call-ups, expected):")
        for match in not_in_squad:
            print(
                f"  id={match.sportmonks_player_id} name={match.sportmonks_name!r} "
                f"dob={match.sportmonks_dob}"
            )
    else:
        print("\nnot_in_current_squad: none")

    no_dob = [m for m in matches if m.method == "no_dob"]
    if no_dob:
        print("\nno_dob:")
        for match in no_dob:
            print(
                f"  id={match.sportmonks_player_id} name={match.sportmonks_name!r}"
            )
    else:
        print("\nno_dob: none")

    if unmatched_squad:
        print("\nUnmatched FIFA squad members (on 26, no rated qualifier row):")
        for row in unmatched_squad:
            print(
                f"  #{row.get('squad_no')} {row.get('player_name')} "
                f"({row.get('position')}) dob={row.get('dob')}"
            )
    else:
        print("\nUnmatched FIFA squad members: none")

    return counts


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("Phase 4.0b: FIFA squad dry-run matcher (corrected)")
    print("=" * 70)

    if not SQUAD_CSV.exists():
        print(f"\n[ERROR] Missing squad CSV at {SQUAD_CSV}")
        return 1

    squad_rows = load_squad_csv(SQUAD_CSV)
    print(f"Loaded {len(squad_rows)} squad rows from {SQUAD_CSV.name}")

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    all_counts: dict[str, dict[str, int]] = {}

    try:
        for team in SAMPLE_TEAMS:
            team_code = team["team_code"]
            team_id = int(team["team_id"])
            search_name = str(team["search_name"])
            team_squad = squad_for_team(squad_rows, team_code)
            print(
                f"\nFetching fixtures for {search_name} "
                f"(team_id={team_id}, squad={len(team_squad)} players)..."
            )

            fixtures = fetch_recent_fixtures_with_lineups(
                client, team_id, limit=config.MATCH_WINDOW_SIZE
            )
            print(f"  fixtures with lineups: {len(fixtures)}")

            if not fixtures:
                print("  [WARNING] No fixtures returned; skipping matcher.")
                all_counts[team_code] = {"no_fixtures": 1}
                continue

            agg = aggregate_player_ratings(team_id, fixtures)
            rated = rated_players_from_result(agg)
            print(
                f"  rated players: {len(rated)} "
                f"(listed={len(agg.listed)}, insufficient={len(agg.insufficient_data)})"
            )
            with_dob = sum(1 for row in rated if row.get("dob"))
            print(f"  rated players with DOB: {with_dob}/{len(rated)}")

            matches, unmatched_squad = match_rated_players_to_squad(
                rated, team_squad
            )
            all_counts[team_code] = print_team_report(
                team_code, search_name, team_squad, matches, unmatched_squad
            )

        print("\n" + "=" * 70)
        print("SUMMARY (method counts per team)")
        print("=" * 70)
        for team_code, counts in all_counts.items():
            print(f"  {team_code}: {counts}")

        print("\n[OK] Dry-run complete.")
        return 0

    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
