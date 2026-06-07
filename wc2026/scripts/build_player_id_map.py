"""Build SportMonks -> FIFA squad player_id_map for all 48 WC teams.

Reads wc2026_squads from Neon, fetches recent fixtures per team, runs the
corrected matcher, and writes player_id_map + player_match_review.

Run from the project root (after load_squads_to_neon.py):

    py -3 scripts/build_player_id_map.py
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import (  # noqa: E402
    DatabaseError,
    get_wc2026_squad_for_team,
    get_wc2026_squad_team_codes,
    replace_player_match_review_for_team,
    upsert_player_id_map_row,
)
from src.player_ratings import (  # noqa: E402
    SquadMatchResult,
    aggregate_player_ratings,
    fetch_lineup_fixtures_for_team,
    match_rated_players_to_squad,
)
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

logger = logging.getLogger("build_player_id_map")

_ROSTER_PATH = _PROJECT_ROOT / "data" / "wc2026_teams.json"
_HOST_CODES = frozenset({"USA", "CAN", "MEX"})
_MAP_METHODS = frozenset({"dob+name", "dob_only"})
_REVIEW_METHODS = frozenset({"unmatched_ambiguous", "not_in_current_squad", "no_dob"})

# SportMonks teams/search query per FIFA team_code (when roster JSON is insufficient).
_SEARCH_NAMES: dict[str, str] = {
    "USA": "United States",
    "CAN": "Canada",
    "MEX": "Mexico",
    "CIV": "Cote",
    "CPV": "Cape Verde Islands",
    "CUW": "Curacao",
    "CZE": "Czech Republic",
    "TUR": "Turkiye",
    "COD": "Congo DR",
    "JOR": "Jordan",
    "SCO": "Scotland",
    "IRN": "Iran",
    "KOR": "Korea Republic",
    "BIH": "Bosnia and Herzegovina",
}


@dataclass
class TeamSummary:
    team_code: str
    team_name: str
    team_id: int | None = None
    rated_players: int = 0
    fixture_count: int = 0
    counts: dict[str, int] = field(default_factory=dict)
    dob_only_rows: list[SquadMatchResult] = field(default_factory=list)
    ambiguous_rows: list[SquadMatchResult] = field(default_factory=list)
    squad_unmatched: list[dict[str, Any]] = field(default_factory=list)
    host_note: str | None = None
    error: str | None = None


def _load_roster() -> list[dict[str, Any]]:
    payload = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    return list(payload.get("teams", []))


_TEAM_CODE_SEARCH_FALLBACKS: dict[str, list[str]] = {
    "COD": ["Congo DR", "DR Congo", "Democratic Republic of the Congo"],
}


def _resolve_team_id(
    client: SportmonksClient,
    team_code: str,
    team_name: str,
    roster: list[dict[str, Any]],
) -> int | None:
    search_names = [_SEARCH_NAMES.get(team_code, team_name)]
    search_names.extend(_TEAM_CODE_SEARCH_FALLBACKS.get(team_code, []))

    for search_name in search_names:
        for entry in roster:
            if entry.get("search_name", "").lower() == search_name.lower():
                team_id = entry.get("sportmonks_team_id")
                if team_id is not None:
                    return int(team_id)

        try:
            response = client.get(f"teams/search/{search_name}")
            candidates = response.get("data")
            if not isinstance(candidates, list) or not candidates:
                continue
            nationals = [t for t in candidates if t.get("type") == "national"]
            chosen = nationals[0] if nationals else candidates[0]
            return int(chosen["id"])
        except SportmonksError:
            continue
    return None


def _squad_row_to_matcher_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "squad_no": str(row["squad_no"]),
        "player_name": row.get("player_name", ""),
        "last_names": row.get("last_names", ""),
        "name_on_shirt": row.get("name_on_shirt", ""),
        "first_names": row.get("first_names", ""),
        "dob": str(row["dob"]) if row.get("dob") else None,
        "position": row.get("position", ""),
    }


def _rated_players(result) -> list[dict[str, Any]]:
    return list(result.listed) + list(result.insufficient_data)


def _process_team(
    client: SportmonksClient,
    team_code: str,
    roster: list[dict[str, Any]],
) -> TeamSummary:
    squad_db = get_wc2026_squad_for_team(team_code)
    team_name = str(squad_db[0]["team_name"]) if squad_db else team_code
    summary = TeamSummary(team_code=team_code, team_name=team_name)

    if not squad_db:
        summary.error = "no squad rows in Neon"
        return summary

    team_id = _resolve_team_id(client, team_code, team_name, roster)
    summary.team_id = team_id
    if team_id is None:
        summary.error = "could not resolve SportMonks team_id"
        return summary

    fixtures = fetch_lineup_fixtures_for_team(client, team_id)
    summary.fixture_count = len(fixtures)

    if team_code in _HOST_CODES and not fixtures:
        summary.host_note = (
            "host nation with no competitive fixtures yet; empty map expected"
        )
        replace_player_match_review_for_team(team_code, [])
        return summary

    if not fixtures:
        summary.error = "no fixtures returned from SportMonks"
        return summary

    squad_rows = [_squad_row_to_matcher_dict(row) for row in squad_db]
    agg = aggregate_player_ratings(team_id, fixtures)
    rated = _rated_players(agg)
    summary.rated_players = len(rated)

    matches, unmatched_squad = match_rated_players_to_squad(rated, squad_rows)
    summary.squad_unmatched = [
        {
            "squad_no": row.get("squad_no"),
            "player_name": row.get("player_name"),
            "dob": row.get("dob"),
            "position": row.get("position"),
        }
        for row in unmatched_squad
    ]

    review_rows: list[dict[str, Any]] = []
    for match in matches:
        summary.counts[match.method] = summary.counts.get(match.method, 0) + 1

        if match.method in _MAP_METHODS and match.squad_no is not None:
            upsert_player_id_map_row(
                sportmonks_player_id=match.sportmonks_player_id,
                team_code=team_code,
                squad_no=int(match.squad_no),
                match_method=match.method,
                match_confidence=match.confidence,
            )
            if match.method == "dob_only":
                summary.dob_only_rows.append(match)
        elif match.method in _REVIEW_METHODS:
            review_rows.append(
                {
                    "sportmonks_player_id": match.sportmonks_player_id,
                    "sm_name": match.sportmonks_name,
                    "sm_dob": match.sportmonks_dob,
                    "reason": match.method,
                    "detail": None,
                }
            )
            if match.method == "unmatched_ambiguous":
                summary.ambiguous_rows.append(match)

    summary.counts["squad_unmatched"] = len(unmatched_squad)
    replace_player_match_review_for_team(team_code, review_rows)
    return summary


def _print_field_summary(summaries: list[TeamSummary]) -> None:
    print("\n" + "=" * 70)
    print("FIELD-WIDE SUMMARY")
    print("=" * 70)

    header = (
        f"{'code':<4} {'dob+name':>8} {'dob_only':>8} {'ambig':>6} "
        f"{'not_squad':>9} {'no_dob':>6} {'sq_unm':>7} {'rated':>5} {'fix':>3}"
    )
    print(header)
    print("-" * len(header))

    all_dob_only: list[SquadMatchResult] = []
    all_ambiguous: list[tuple[str, SquadMatchResult]] = []
    suspicious: list[str] = []

    for summary in sorted(summaries, key=lambda s: s.team_code):
        if summary.error:
            print(
                f"{summary.team_code:<4} ERROR: {summary.error} "
                f"(team_id={summary.team_id})"
            )
            continue

        c = summary.counts
        print(
            f"{summary.team_code:<4} "
            f"{c.get('dob+name', 0):>8} "
            f"{c.get('dob_only', 0):>8} "
            f"{c.get('unmatched_ambiguous', 0):>6} "
            f"{c.get('not_in_current_squad', 0):>9} "
            f"{c.get('no_dob', 0):>6} "
            f"{c.get('squad_unmatched', 0):>7} "
            f"{summary.rated_players:>5} "
            f"{summary.fixture_count:>3}"
        )
        if summary.host_note:
            print(f"      note: {summary.host_note}")

        all_dob_only.extend(summary.dob_only_rows)
        for row in summary.ambiguous_rows:
            all_ambiguous.append((summary.team_code, row))

        if (
            c.get("dob+name", 0) == 0
            and summary.team_code not in _HOST_CODES
            and summary.fixture_count > 0
            and summary.rated_players > 0
        ):
            suspicious.append(summary.team_code)
        elif (
            c.get("dob+name", 0) == 0
            and summary.team_code not in _HOST_CODES
            and summary.fixture_count > 0
            and summary.rated_players == 0
        ):
            print(
                f"      note: fixtures present but zero rated players "
                f"(SportMonks coverage gap, not a matcher issue)"
            )

    print("\n--- dob_only across all teams ---")
    if all_dob_only:
        for match in all_dob_only:
            print(
                f"  SM={match.sportmonks_name!r} dob={match.sportmonks_dob} "
                f"-> squad={match.squad_player_name!r} dob={match.squad_dob}"
            )
    else:
        print("  none")

    print("\n--- unmatched_ambiguous (multi-DOB ties) ---")
    if all_ambiguous:
        for team_code, match in all_ambiguous:
            print(
                f"  [{team_code}] id={match.sportmonks_player_id} "
                f"name={match.sportmonks_name!r} dob={match.sportmonks_dob}"
            )
    else:
        print("  none")

    print("\n--- SUSPICIOUS (0 dob+name, non-host, has fixtures) ---")
    if suspicious:
        for code in suspicious:
            print(f"  {code}")
    else:
        print("  none")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("BUILD player_id_map (48 teams)")
    print("=" * 70)

    try:
        team_codes = get_wc2026_squad_team_codes()
    except DatabaseError as exc:
        print(f"ERROR: could not read squads from Neon: {exc}")
        print("Run scripts/load_squads_to_neon.py first.")
        return 1

    print(f"Teams in Neon wc2026_squads: {len(team_codes)}")
    if len(team_codes) != 48:
        print(f"WARNING: expected 48 teams, found {len(team_codes)}")

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"ERROR: SportMonks client: {exc}")
        return 1

    roster = _load_roster()
    summaries: list[TeamSummary] = []

    try:
        for team_code in team_codes:
            print(f"\nProcessing {team_code}...")
            summaries.append(_process_team(client, team_code, roster))
    except DatabaseError as exc:
        print(f"\nERROR: database write failed: {exc}")
        return 1

    _print_field_summary(summaries)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
