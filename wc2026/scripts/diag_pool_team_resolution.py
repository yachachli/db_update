"""Inspect pool vs ratings team_id resolution for WC fixtures (read-only).

Phase 4 pre-tournament: identify fixtures skipped by predict_matchup_by_id
because a SportMonks team_id is not rostered in the tournament pool.

    py -3 scripts/diag_pool_team_resolution.py

STOP after printing the proposed resolution table — no writes.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.database import (  # noqa: E402
    DatabaseError,
    get_connection,
    get_fifa_code_for_team_id,
    get_team_id_for_fifa_code,
    get_wc2026_squad_for_team,
)
from src.pipeline import (  # noqa: E402
    TournamentPool,
    _load_roster,
    _resolve_team_by_id,
    bootstrap_tournament_pool,
)
from src.sportmonks_client import SportmonksClient  # noqa: E402

# Reuse 4.2a ratings-path resolver (single source of truth for Neon team_id lookup).
import importlib.util  # noqa: E402

_resolve_mod_path = _PROJECT_ROOT / "scripts" / "resolve_missing_team_ids.py"
_spec = importlib.util.spec_from_file_location("resolve_missing_team_ids", _resolve_mod_path)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load {_resolve_mod_path}")
_resolve_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_resolve_mod)
_resolve_team_id = _resolve_mod._resolve_team_id

_ROSTER_PATH = _PROJECT_ROOT / "data" / "wc2026_teams.json"


def _pool_rostered_ids(pool: TournamentPool) -> set[int]:
    return set(pool.host_ratings.keys()) | set(pool.matches_by_team.keys())


def _pool_status(team_id: int, pool: TournamentPool) -> str:
    if team_id in pool.host_ratings:
        return "in_pool:host"
    if team_id in pool.matches_by_team and team_id in pool.teams:
        return "in_pool:qualifier"
    if team_id in pool.teams:
        return "in_pool.teams_only (NOT predictable)"
    return "NOT in pool"


def _roster_entry_for_id(team_id: int, roster: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in roster:
        rid = entry.get("sportmonks_team_id")
        if rid is not None and int(rid) == team_id:
            return entry
    return None


def _fixture_team_codes(
    team_a_id: int | None,
    team_b_id: int | None,
) -> tuple[str | None, str | None]:
    return (
        get_fifa_code_for_team_id(int(team_a_id)) if team_a_id is not None else None,
        get_fifa_code_for_team_id(int(team_b_id)) if team_b_id is not None else None,
    )


def _ratings_resolve(
    client: SportmonksClient,
    team_code: str | None,
    team_name: str,
    roster: list[dict[str, Any]],
) -> tuple[int | None, str]:
    if not team_code:
        return None, "no_fifa_code"
    existing = get_team_id_for_fifa_code(team_code)
    if existing is not None:
        return existing, "neon:teams.fifa_code"
    team_id, method = _resolve_team_id(client, team_code, team_name, roster)
    return team_id, method


def _try_pool_resolve(team_id: int, pool: TournamentPool) -> str | None:
    try:
        _resolve_team_by_id(team_id, pool)
        return None
    except ValueError as exc:
        return str(exc)


def _load_fixtures() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT fixture_id, team_a_id, team_b_id, team_a_name, team_b_name,
                   scheduled_at, status
            FROM wc2026_fixtures
            ORDER BY scheduled_at ASC, fixture_id ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _squad_name_for_code(team_code: str) -> str:
    squad = get_wc2026_squad_for_team(team_code)
    if squad:
        return str(squad[0].get("team_name") or team_code)
    return team_code


def main() -> int:
    print("=" * 78)
    print("DIAG: POOL TEAM RESOLUTION (read-only)")
    print("=" * 78)

    try:
        pool = bootstrap_tournament_pool()
        fixtures = _load_fixtures()
    except DatabaseError as exc:
        print(f"ERROR: {exc}")
        return 1

    roster = _load_roster()
    client = SportmonksClient()
    rostered_ids = _pool_rostered_ids(pool)

    print(f"\nPool: {len(pool.teams)} teams loaded, {len(rostered_ids)} predictable (rostered).")
    if pool.failed_teams:
        print(f"Bootstrap failures ({len(pool.failed_teams)}): {', '.join(pool.failed_teams)}")

    failing_fixtures: list[dict[str, Any]] = []
    failing_sides: list[dict[str, Any]] = []

    print("\n--- FIXTURES WITH POOL RESOLUTION FAILURE ---\n")
    for fx in fixtures:
        fid = fx["fixture_id"]
        a_id = fx.get("team_a_id")
        b_id = fx.get("team_b_id")
        a_name = fx.get("team_a_name") or ""
        b_name = fx.get("team_b_name") or ""

        if a_id is None or b_id is None:
            print(f"fixture {fid}: {a_name} vs {b_name} — SKIP (missing Neon team ids)")
            failing_fixtures.append({**fx, "reason": "missing_neon_ids"})
            continue

        a_err = _try_pool_resolve(int(a_id), pool)
        b_err = _try_pool_resolve(int(b_id), pool)
        if not a_err and not b_err:
            continue

        a_code, b_code = _fixture_team_codes(a_id, b_id)
        sides_failed = []
        if a_err:
            sides_failed.append("team_a")
        if b_err:
            sides_failed.append("team_b")

        print(f"fixture {fid}: {a_name} vs {b_name}")
        print(f"  codes: {a_code or '?'} vs {b_code or '?'}")
        print(f"  fixture ids: {a_id} vs {b_id}")
        print(f"  failed side(s): {', '.join(sides_failed)}")

        for side, tid, tname, tcode, err in (
            ("team_a", a_id, a_name, a_code, a_err),
            ("team_b", b_id, b_name, b_code, b_err),
        ):
            if not err:
                continue
            ratings_id, ratings_method = _ratings_resolve(
                client, tcode, tname, roster,
            )
            roster_entry = _roster_entry_for_id(int(tid), roster)
            print(f"  [{side}] {tname} ({tcode or '?'}) fixture_id={tid}")
            print(f"         pool: {_pool_status(int(tid), pool)} — {err}")
            print(f"         ratings path: team_id={ratings_id} via {ratings_method}")
            if roster_entry:
                print(f"         wc2026_teams.json: search_name={roster_entry.get('search_name')!r} "
                      f"roster_id={roster_entry.get('sportmonks_team_id')}")
            else:
                print("         wc2026_teams.json: NOT LISTED")

            failing_sides.append({
                "fixture_id": fid,
                "side": side,
                "team_code": tcode,
                "team_name": tname,
                "fixture_team_id": int(tid),
                "pool_status": _pool_status(int(tid), pool),
                "pool_error": err,
                "ratings_team_id": ratings_id,
                "ratings_method": ratings_method,
                "in_roster_json": roster_entry is not None,
                "roster_json_id": (
                    int(roster_entry["sportmonks_team_id"])
                    if roster_entry and roster_entry.get("sportmonks_team_id") is not None
                    else None
                ),
            })

        failing_fixtures.append({**fx, "reason": "pool_resolution", "sides": sides_failed})
        print()

    # Proposed resolution table: unique failing team codes -> resolved id -> fixtures
    proposed: dict[str, dict[str, Any]] = {}
    fixtures_by_code: dict[str, set[int]] = defaultdict(set)

    for row in failing_sides:
        code = row["team_code"] or f"UNKNOWN:{row['fixture_team_id']}"
        fixtures_by_code[code].add(row["fixture_id"])
        if code not in proposed:
            squad_name = _squad_name_for_code(code) if not code.startswith("UNKNOWN") else row["team_name"]
            ratings_id, ratings_method = _ratings_resolve(
                client, code if not code.startswith("UNKNOWN") else None,
                squad_name, roster,
            )
            proposed[code] = {
                "team_code": code,
                "squad_name": squad_name,
                "fixture_team_id": row["fixture_team_id"],
                "ratings_team_id": ratings_id,
                "ratings_method": ratings_method,
                "pool_status": row["pool_status"],
                "in_roster_json": row["in_roster_json"],
                "roster_json_id": row["roster_json_id"],
                "id_mismatch": (
                    row["roster_json_id"] is not None
                    and row["roster_json_id"] != row["fixture_team_id"]
                ),
            }

    print("=" * 78)
    print("RESOLVER COMPARISON")
    print("=" * 78)
    print(
        "\nRatings path: get_team_id_for_fifa_code() then _resolve_team_id() "
        "from scripts/resolve_missing_team_ids.py (4.2a).\n"
        "Pool path: bootstrap_tournament_pool() loads wc2026_teams.json only; "
        "predict_matchup_by_id() requires team_id in pool.host_ratings or "
        "pool.matches_by_team via _resolve_team_by_id().\n"
        "Gap: teams resolved in Neon / ratings but absent from pool roster "
        "(or bootstrap failed / wrong id in fixtures vs roster)."
    )

    print("\n" + "=" * 78)
    print("PROPOSED RESOLUTION TABLE (STOP — review before any writes)")
    print("=" * 78)
    if not proposed:
        print("\nNo failing teams — all fixtures resolve in the current pool.")
    else:
        print(
            f"\n{'code':<6} {'fixture_id':>10} {'ratings_id':>10} {'roster_id':>10} "
            f"{'in_json':>8}  fixtures_unblocked"
        )
        print("-" * 78)
        for code in sorted(proposed.keys()):
            p = proposed[code]
            fid_list = sorted(fixtures_by_code[code])
            ratings_id = p["ratings_team_id"]
            roster_id = p["roster_json_id"]
            print(
                f"{code:<6} {p['fixture_team_id']:>10} "
                f"{ratings_id if ratings_id is not None else '-':>10} "
                f"{roster_id if roster_id is not None else '-':>10} "
                f"{'yes' if p['in_roster_json'] else 'no':>8}  "
                f"{fid_list}  ({p['ratings_method']})"
            )
            if p["id_mismatch"]:
                print(
                    f"       NOTE: fixture Neon id {p['fixture_team_id']} != "
                    f"roster json id {roster_id}"
                )
            if not p["in_roster_json"] and ratings_id is not None:
                print(
                    f"       FIX: add {code!r} to wc2026_teams.json with "
                    f"sportmonks_team_id={ratings_id} and re-bootstrap pool"
                )
            elif p["in_roster_json"] and p["pool_status"].startswith("NOT"):
                print(
                    f"       FIX: team in json but not in pool — bootstrap likely "
                    f"failed; check pool.failed_teams / force_refresh"
                )

    print(f"\nFailing fixtures: {len(failing_fixtures)} / {len(fixtures)} total")
    print("STOP — proposed table above; confirm team set before Part 2B apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
