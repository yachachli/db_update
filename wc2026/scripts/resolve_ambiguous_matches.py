"""Inspect and (optionally) resolve unmatched_ambiguous player_match_review rows.

Part 1 (default): print review rows, squad DOB-collision candidates, live SM
rated players for affected teams, and a proposed manual pairing table. No writes.

Part 2 (--apply): upsert confirmed overrides into player_id_map and clear review
rows. Requires data/manual_player_id_overrides.json.

Run from the project root:

    py -3 scripts/resolve_ambiguous_matches.py
    py -3 scripts/resolve_ambiguous_matches.py --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import (  # noqa: E402
    DatabaseError,
    get_connection,
    get_wc2026_squad_for_team,
    upsert_player_id_map_row,
)
from src.player_ratings import (  # noqa: E402
    aggregate_player_ratings,
    fetch_lineup_fixtures_for_team,
    normalize_name_tokens,
    player_ratings_result_to_dict,
)
from src.sportmonks_client import SportmonksClient  # noqa: E402

_OVERRIDE_PATH = _PROJECT_ROOT / "data" / "manual_player_id_overrides.json"
_ROSTER_PATH = _PROJECT_ROOT / "data" / "wc2026_teams.json"
_MANUAL_METHOD = "manual_dob_collision"
_MANUAL_CONFIDENCE = 1.0

# Teams that need a non-default SportMonks search when resolving team_id.
_SEARCH_NAMES: dict[str, str] = {}


@dataclass(frozen=True, slots=True)
class AmbiguousReviewRow:
    sportmonks_player_id: int
    sm_name: str
    sm_dob: str | None
    team_code: str
    detail: str | None


@dataclass(frozen=True, slots=True)
class ProposedOverride:
    sportmonks_player_id: int
    team_code: str
    squad_no: int
    sm_name: str
    sm_dob: str | None
    squad_player_name: str
    surname_basis: str
    status: str


def _load_roster() -> list[dict[str, Any]]:
    payload = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    return list(payload.get("teams", []))


def _team_id_for_code(team_code: str, squad_name: str) -> int | None:
    for entry in _load_roster():
        if entry.get("sportmonks_team_id") is None:
            continue
        search = _SEARCH_NAMES.get(team_code, squad_name)
        if str(entry.get("search_name", "")).lower() == search.lower():
            return int(entry["sportmonks_team_id"])
    for entry in _load_roster():
        name = str(entry.get("search_name", "")).lower()
        if team_code == "ARG" and "argentina" in name:
            return int(entry["sportmonks_team_id"])
    return None


def _fetch_ambiguous_review_rows() -> list[AmbiguousReviewRow]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT sportmonks_player_id, sm_name, sm_dob, team_code, detail
            FROM player_match_review
            WHERE reason = 'unmatched_ambiguous'
            ORDER BY team_code ASC, sm_name ASC
            """
        ).fetchall()
    return [
        AmbiguousReviewRow(
            sportmonks_player_id=int(row["sportmonks_player_id"]),
            sm_name=str(row["sm_name"]),
            sm_dob=str(row["sm_dob"]) if row.get("sm_dob") else None,
            team_code=str(row["team_code"]),
            detail=row.get("detail"),
        )
        for row in rows
    ]


def _squad_candidates_for_dob(team_code: str, dob: str | None) -> list[dict[str, Any]]:
    if not dob:
        return []
    return [
        row
        for row in get_wc2026_squad_for_team(team_code)
        if str(row.get("dob", "")) == dob
    ]


def _squad_surname_tokens(squad_row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for field in ("last_names", "name_on_shirt"):
        tokens |= normalize_name_tokens(str(squad_row.get(field, "")))
    return tokens


def _propose_pairing(
    review: AmbiguousReviewRow,
    candidates: list[dict[str, Any]],
) -> ProposedOverride | None:
    sm_tokens = normalize_name_tokens(review.sm_name)
    hits: list[tuple[dict[str, Any], set[str]]] = []
    for row in candidates:
        surname_tokens = _squad_surname_tokens(row)
        overlap = surname_tokens & sm_tokens
        if overlap:
            hits.append((row, overlap))

    if len(hits) != 1:
        return ProposedOverride(
            sportmonks_player_id=review.sportmonks_player_id,
            team_code=review.team_code,
            squad_no=-1,
            sm_name=review.sm_name,
            sm_dob=review.sm_dob,
            squad_player_name="",
            surname_basis="",
            status=(
                "UNRESOLVED"
                if not hits
                else f"AMBIGUOUS ({len(hits)} surname hits)"
            ),
        )

    row, overlap = hits[0]
    return ProposedOverride(
        sportmonks_player_id=review.sportmonks_player_id,
        team_code=review.team_code,
        squad_no=int(row["squad_no"]),
        sm_name=review.sm_name,
        sm_dob=review.sm_dob,
        squad_player_name=str(row.get("player_name", "")),
        surname_basis=", ".join(sorted(overlap)),
        status="PROPOSED",
    )


def _count_ambiguous_review_rows() -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM player_match_review
            WHERE reason = 'unmatched_ambiguous'
            """
        ).fetchone()
    return int(row["n"]) if row else 0


def _count_id_map_rows(team_code: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM player_id_map WHERE team_code = %s",
            (team_code,),
        ).fetchone()
    return int(row["n"]) if row else 0


def _squad_row_for_no(team_code: str, squad_no: int) -> dict[str, Any] | None:
    for row in get_wc2026_squad_for_team(team_code):
        if int(row["squad_no"]) == squad_no:
            return row
    return None


def _load_overrides() -> list[dict[str, Any]]:
    if not _OVERRIDE_PATH.exists():
        raise FileNotFoundError(f"Missing override file: {_OVERRIDE_PATH}")
    payload = json.loads(_OVERRIDE_PATH.read_text(encoding="utf-8"))
    overrides = payload.get("overrides", [])
    if not isinstance(overrides, list) or not overrides:
        raise ValueError(f"No overrides listed in {_OVERRIDE_PATH}")
    return overrides


def _validate_override(entry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (normalized entry, squad row) or raise if surnames disagree."""
    team_code = str(entry["team_code"])
    squad_no = int(entry["squad_no"])
    sm_name = str(entry.get("sm_name", ""))
    sportmonks_player_id = int(entry["sportmonks_player_id"])

    squad_row = _squad_row_for_no(team_code, squad_no)
    if squad_row is None:
        raise ValueError(
            f"squad_no {squad_no} does not exist for team_code {team_code}"
        )

    sm_tokens = normalize_name_tokens(sm_name)
    surname_tokens = _squad_surname_tokens(squad_row)
    overlap = surname_tokens & sm_tokens
    if not overlap:
        raise ValueError(
            f"Surname mismatch for sm_id={sportmonks_player_id} {sm_name!r} -> "
            f"#{squad_no} {squad_row.get('player_name')!r} "
            f"(last_names={squad_row.get('last_names')!r}, "
            f"shirt={squad_row.get('name_on_shirt')!r})"
        )

    return (
        {
            "sportmonks_player_id": sportmonks_player_id,
            "team_code": team_code,
            "squad_no": squad_no,
            "sm_name": sm_name,
            "surname_basis": ", ".join(sorted(overlap)),
        },
        squad_row,
    )


def _delete_resolved_review_rows(entries: list[dict[str, Any]]) -> int:
    deleted = 0
    with get_connection() as conn:
        with conn.transaction():
            for entry in entries:
                result = conn.execute(
                    """
                    DELETE FROM player_match_review
                    WHERE sportmonks_player_id = %s
                      AND team_code = %s
                      AND reason = 'unmatched_ambiguous'
                    """,
                    (entry["sportmonks_player_id"], entry["team_code"]),
                )
                deleted += int(result.rowcount or 0)
    return deleted


def _fetch_id_map_entry(
    sportmonks_player_id: int,
) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT m.sportmonks_player_id, m.team_code, m.squad_no,
                   m.match_method, m.match_confidence, s.player_name
            FROM player_id_map m
            JOIN wc2026_squads s
              ON s.team_code = m.team_code AND s.squad_no = m.squad_no
            WHERE m.sportmonks_player_id = %s
            """,
            (sportmonks_player_id,),
        ).fetchone()
    return dict(row) if row else None


def _print_candidate_row(row: dict[str, Any]) -> None:
    print(
        f"      #{row['squad_no']:>2}  {row.get('player_name', '?'):<22}  "
        f"last_names={row.get('last_names', '')!r}  "
        f"shirt={row.get('name_on_shirt', '')!r}  "
        f"pos={row.get('position', '')}"
    )


def _fetch_live_rated_players(team_code: str) -> list[dict[str, Any]]:
    squad = get_wc2026_squad_for_team(team_code)
    team_name = str(squad[0]["team_name"]) if squad else team_code
    team_id = _team_id_for_code(team_code, team_name)
    if team_id is None:
        print(f"  WARNING: could not resolve SportMonks team_id for {team_code}")
        return []

    client = SportmonksClient()
    fixtures = fetch_lineup_fixtures_for_team(client, team_id)
    if not fixtures:
        print(f"  WARNING: no fixtures with lineups for {team_code} (team_id={team_id})")
        return []

    result = player_ratings_result_to_dict(
        aggregate_player_ratings(team_id, fixtures)
    )
    return list(result["listed"]) + list(result["insufficient_data"])


def _print_review_section(review_rows: list[AmbiguousReviewRow]) -> None:
    print("\n" + "=" * 78)
    print("UNMATCHED_AMBIGUOUS REVIEW ROWS (player_match_review)")
    print("=" * 78)
    if not review_rows:
        print("  (none)")
        return

    for review in review_rows:
        print(
            f"\n  team={review.team_code}  sm_id={review.sportmonks_player_id}  "
            f"name={review.sm_name!r}  dob={review.sm_dob}"
        )
        if review.detail:
            print(f"    detail: {review.detail}")

        candidates = _squad_candidates_for_dob(review.team_code, review.sm_dob)
        print(f"    squad candidates sharing dob={review.sm_dob}:")
        if not candidates:
            print("      (none in wc2026_squads)")
        else:
            for row in candidates:
                _print_candidate_row(row)


def _print_arg_live_collision(dob: str) -> None:
    print("\n" + "=" * 78)
    print(f"ARG LIVE RATED PLAYERS (dob={dob})")
    print("=" * 78)
    rated = _fetch_live_rated_players("ARG")
    collision = [p for p in rated if str(p.get("dob", "")) == dob]
    if not collision:
        print("  (no rated ARG players with this DOB in current fixture window)")
        print(f"  total rated ARG players fetched: {len(rated)}")
        return

    for player in sorted(collision, key=lambda p: p["player_name"].lower()):
        print(
            f"  sm_id={player['player_id']:>10}  "
            f"name={player['player_name']!r}  "
            f"dob={player.get('dob')}  "
            f"avg={player.get('avg_rating')}  "
            f"n={player.get('matches_counted')}"
        )

    already_mapped = _arg_mapped_ids_for_dob_players(collision)
    if already_mapped:
        print("\n  Already in player_id_map among this DOB cohort:")
        for entry in already_mapped:
            print(
                f"    sm_id={entry['sportmonks_player_id']} -> "
                f"#{entry['squad_no']} {entry['player_name']} "
                f"({entry['match_method']})"
            )


def _arg_mapped_ids_for_dob_players(
    rated_players: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ids = [int(p["player_id"]) for p in rated_players]
    if not ids:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT m.sportmonks_player_id, m.squad_no, m.match_method,
                   s.player_name
            FROM player_id_map m
            JOIN wc2026_squads s
              ON s.team_code = m.team_code AND s.squad_no = m.squad_no
            WHERE m.team_code = 'ARG'
              AND m.sportmonks_player_id = ANY(%s)
            ORDER BY m.squad_no ASC
            """,
            (ids,),
        ).fetchall()
    return [dict(row) for row in rows]


def _print_proposed_table(
    review_rows: list[AmbiguousReviewRow],
    proposals: list[ProposedOverride],
) -> None:
    print("\n" + "=" * 78)
    print("PROPOSED MANUAL PAIRINGS (inspect only — no DB writes in Part 1)")
    print("=" * 78)
    print(
        f"{'team':<5} {'sm_id':>10} {'sm_name':<22} {'dob':<12} "
        f"{'->':^3} {'sq':>3} {'squad_name':<22} {'basis':<12} {'status'}"
    )
    print("-" * 78)
    for review, proposal in zip(review_rows, proposals, strict=True):
        arrow = "->"
        sq = str(proposal.squad_no) if proposal.squad_no > 0 else "?"
        print(
            f"{review.team_code:<5} {review.sportmonks_player_id:>10} "
            f"{review.sm_name:<22} {str(review.sm_dob or ''):<12} "
            f"{arrow:^3} {sq:>3} {proposal.squad_player_name:<22} "
            f"{proposal.surname_basis:<12} {proposal.status}"
        )

    unresolved = [p for p in proposals if p.status != "PROPOSED"]
    if unresolved:
        print(
            f"\n  NOTE: {len(unresolved)} row(s) could not be auto-proposed by "
            "surname — confirm manually before --apply."
        )
    else:
        print(
            "\n  All ambiguous rows have a single surname-resolved proposal. "
            "Confirm pairings, then re-run with --apply (Part 2)."
        )


def inspect() -> int:
    print("=" * 78)
    print("RESOLVE AMBIGUOUS MATCHES — PART 1 (inspect only, no DB writes)")
    print("=" * 78)

    try:
        review_rows = _fetch_ambiguous_review_rows()
        ambiguous_before = _count_ambiguous_review_rows()
        arg_map_before = _count_id_map_rows("ARG")
    except DatabaseError as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("\nCounts before any changes:")
    print(f"  unmatched_ambiguous review rows: {ambiguous_before}")
    print(f"  player_id_map rows for ARG:      {arg_map_before}")

    _print_review_section(review_rows)

    arg_dobs = {r.sm_dob for r in review_rows if r.team_code == "ARG" and r.sm_dob}
    for dob in sorted(arg_dobs):
        _print_arg_live_collision(dob)

    proposals: list[ProposedOverride] = []
    for review in review_rows:
        candidates = _squad_candidates_for_dob(review.team_code, review.sm_dob)
        proposals.append(_propose_pairing(review, candidates))

    _print_proposed_table(review_rows, proposals)

    print("\nSTOP — Part 1 complete. No database changes made.")
    print("Confirm proposed pairings, then re-run with --apply (Part 2).")
    return 0


def apply() -> int:
    print("=" * 78)
    print("RESOLVE AMBIGUOUS MATCHES — PART 2 (apply manual overrides)")
    print("=" * 78)

    try:
        overrides = _load_overrides()
        ambiguous_before = _count_ambiguous_review_rows()
        arg_map_before = _count_id_map_rows("ARG")
    except (DatabaseError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"\nERROR: {exc}")
        return 1

    print("\nCounts before apply:")
    print(f"  unmatched_ambiguous review rows: {ambiguous_before}")
    print(f"  player_id_map rows for ARG:      {arg_map_before}")
    print(f"  overrides to apply:              {len(overrides)}")
    print(f"  override file:                   {_OVERRIDE_PATH}")

    applied: list[dict[str, Any]] = []
    try:
        for raw in overrides:
            entry, squad_row = _validate_override(raw)
            upsert_player_id_map_row(
                sportmonks_player_id=entry["sportmonks_player_id"],
                team_code=entry["team_code"],
                squad_no=entry["squad_no"],
                match_method=_MANUAL_METHOD,
                match_confidence=_MANUAL_CONFIDENCE,
            )
            applied.append(entry)
            print(
                f"\n  UPSERT player_id_map: sm_id={entry['sportmonks_player_id']} "
                f"{entry['sm_name']!r} -> {entry['team_code']} "
                f"#{entry['squad_no']} {squad_row.get('player_name')} "
                f"(basis={entry['surname_basis']}, method={_MANUAL_METHOD})"
            )
    except (DatabaseError, ValueError) as exc:
        print(f"\nERROR during apply (rolled back per-row txn): {exc}")
        return 1

    deleted = _delete_resolved_review_rows(applied)

    ambiguous_after = _count_ambiguous_review_rows()
    arg_map_after = _count_id_map_rows("ARG")

    print("\n" + "=" * 78)
    print("AFTER APPLY")
    print("=" * 78)
    print(f"  unmatched_ambiguous review rows: {ambiguous_before} -> {ambiguous_after}")
    print(f"  player_id_map rows for ARG:      {arg_map_before} -> {arg_map_after}")
    print(f"  review rows deleted:               {deleted}")

    print("\nApplied mappings (verified in player_id_map):")
    for entry in applied:
        stored = _fetch_id_map_entry(entry["sportmonks_player_id"])
        if not stored:
            print(f"  WARNING: sm_id={entry['sportmonks_player_id']} not found after upsert")
            continue
        print(
            f"  sm_id={stored['sportmonks_player_id']} -> "
            f"{stored['team_code']} #{stored['squad_no']} "
            f"{stored['player_name']} "
            f"method={stored['match_method']} "
            f"confidence={stored['match_confidence']}"
        )

    print("\nDone.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Part 2: apply confirmed overrides from manual_player_id_overrides.json",
    )
    args = parser.parse_args()

    if args.apply:
        return apply()

    return inspect()


if __name__ == "__main__":
    raise SystemExit(main())
