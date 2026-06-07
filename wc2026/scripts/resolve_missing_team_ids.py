"""Resolve and persist SportMonks team_ids for WC squads missing from Neon teams.

Phase 4.2a FIX 2: ALG, HAI, PAR, RSA (same resolution class as COD).

    py -3 scripts/resolve_missing_team_ids.py
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
    get_player_ratings_snapshot_summary,
    get_team_id_for_fifa_code,
    get_wc2026_squad_for_team,
    get_wc2026_squad_team_codes,
    upsert_team,
)
from src.models import Team  # noqa: E402
from src.player_ratings import (  # noqa: E402
    aggregate_player_ratings,
    fetch_lineup_fixtures_for_team,
    snapshot_player_ratings_for_pool,
)
from src.sportmonks_client import SportmonksClient, SportmonksError  # noqa: E402

_ROSTER_PATH = _PROJECT_ROOT / "data" / "wc2026_teams.json"
_HOST_CODES = frozenset({"USA", "CAN", "MEX"})

_TARGET_CODES = ("ALG", "HAI", "PAR", "RSA")

_SEARCH_NAMES: dict[str, str] = {
    "RSA": "South Africa",
}

_TEAM_CODE_SEARCH_FALLBACKS: dict[str, list[str]] = {
    "ALG": ["Algeria"],
    "HAI": ["Haiti"],
    "PAR": ["Paraguay"],
    "RSA": ["South Africa", "RSA"],
}


def _load_roster() -> list[dict[str, Any]]:
    return list(json.loads(_ROSTER_PATH.read_text(encoding="utf-8")).get("teams", []))


def _resolve_team_id(
    client: SportmonksClient,
    team_code: str,
    team_name: str,
    roster: list[dict[str, Any]],
) -> tuple[int | None, str]:
    """Return (team_id, resolution_method)."""
    search_names = [_SEARCH_NAMES.get(team_code, team_name)]
    search_names.extend(_TEAM_CODE_SEARCH_FALLBACKS.get(team_code, []))

    for search_name in search_names:
        for entry in roster:
            if str(entry.get("search_name", "")).lower() == search_name.lower():
                team_id = entry.get("sportmonks_team_id")
                if team_id is not None:
                    return int(team_id), f"roster:{search_name}"

        try:
            response = client.get(f"teams/search/{search_name}")
            candidates = response.get("data")
            if not isinstance(candidates, list) or not candidates:
                continue
            nationals = [t for t in candidates if t.get("type") == "national"]
            chosen = nationals[0] if nationals else candidates[0]
            return int(chosen["id"]), f"search:{search_name}"
        except SportmonksError:
            continue
    return None, "unresolved"


def _upsert_team_row(
    team_id: int,
    team_code: str,
    team_name: str,
    confederation: str,
) -> None:
    upsert_team(
        Team(
            team_id=team_id,
            name=team_name,
            confederation=confederation,
            fifa_points=0.0,
            fifa_rank=999,
            is_host=team_code in _HOST_CODES,
        ),
        fifa_code=team_code,
    )


def _confederation_for_code(team_code: str, roster: list[dict[str, Any]], team_id: int) -> str:
    for entry in roster:
        if entry.get("sportmonks_team_id") == team_id:
            return str(entry.get("confederation", "UNKNOWN"))
    return "UNKNOWN"


def _rated_count(team_id: int, fixtures: list[dict[str, Any]]) -> int:
    if not fixtures:
        return 0
    result = aggregate_player_ratings(team_id, fixtures)
    return len(result.listed) + len(result.insufficient_data)


def _teams_in_current_snapshot(snapshot_date: str) -> dict[str, dict[str, int]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT team_code, source, COUNT(*) AS n
            FROM player_ratings_history
            WHERE snapshot_date = %s
            GROUP BY team_code, source
            """,
            (snapshot_date,),
        ).fetchall()
    out: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        out[str(row["team_code"])][str(row["source"])] = int(row["n"])
    return dict(out)


def _print_verdict(snapshot_date: str) -> None:
    allowed = sorted(get_wc2026_squad_team_codes())
    present = _teams_in_current_snapshot(snapshot_date)

    print("\n" + "=" * 78)
    print("PHASE 4.2a VERDICT")
    print("=" * 78)

    present_codes = sorted(present.keys())
    absent = [c for c in allowed if c not in present]

    total_entities = 0
    by_source: dict[str, int] = defaultdict(int)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT entity_key) AS n
            FROM player_ratings_current
            """
        ).fetchone()
        total_entities = int(row["n"]) if row else 0
        src_rows = conn.execute(
            """
            SELECT source, COUNT(*) AS n
            FROM player_ratings_current
            GROUP BY source
            ORDER BY source ASC
            """
        ).fetchall()
        for sr in src_rows:
            by_source[str(sr["source"])] = int(sr["n"])

    print(f"\nPresent in today's snapshot ({len(present_codes)}/48):")
    for code in present_codes:
        sources = present[code]
        detail = ", ".join(f"{k}={v}" for k, v in sorted(sources.items()))
        print(f"  {code:<4} {detail}")

    print(f"\nAbsent from today's snapshot ({len(absent)}):")
    for code in absent:
        reason = _absence_reason(code)
        print(f"  {code:<4} {reason}")

    orphan = sorted(set(present_codes) - set(allowed))
    if orphan:
        print(f"\nWARNING: non-squad codes in snapshot: {orphan}")

    print(f"\nplayer_ratings_current:")
    print(f"  distinct entity groups: {total_entities}")
    print(f"  source breakdown: {dict(by_source)}")
    print("\nSTOP — 4.2a complete. Await confirmation before 4.2 XI builder.")


def _absence_reason(team_code: str) -> str:
    if team_code in _HOST_CODES:
        return "pre-tournament host (may have limited/no competitive window yet)"
    if team_code == "NZL":
        return "should be manual — re-run snapshot if missing"
    if team_code in _TARGET_CODES:
        tid = get_team_id_for_fifa_code(team_code)
        if tid is None:
            return "still unresolved (no team_id in Neon)"
    return "no snapshot rows today — check fixtures/ratings"


def main() -> int:
    print("=" * 78)
    print("RESOLVE MISSING TEAM IDs (ALG, HAI, PAR, RSA)")
    print("=" * 78)

    roster = _load_roster()
    client = SportmonksClient()

    try:
        for team_code in _TARGET_CODES:
            squad = get_wc2026_squad_for_team(team_code)
            team_name = str(squad[0]["team_name"]) if squad else team_code

            existing = get_team_id_for_fifa_code(team_code)
            team_id, method = _resolve_team_id(client, team_code, team_name, roster)

            print(f"\n{team_code} ({team_name}):")
            if team_id is None:
                print("  ERROR: could not resolve SportMonks team_id")
                continue

            conf = _confederation_for_code(team_code, roster, team_id)
            _upsert_team_row(team_id, team_code, team_name, conf)
            print(f"  resolved team_id={team_id} via {method}")
            if existing and existing != team_id:
                print(f"  NOTE: replaced prior Neon mapping {existing} -> {team_id}")

            fixtures = fetch_lineup_fixtures_for_team(client, team_id)
            rated = _rated_count(team_id, fixtures)
            sm_name = team_name
            try:
                for entry in roster:
                    if int(entry.get("sportmonks_team_id", -1)) == team_id:
                        sm_name = str(entry.get("search_name", team_name))
                        break
            except (TypeError, ValueError):
                pass
            print(f"  name={sm_name!r}  fixtures={len(fixtures)}  rated_players={rated}")
            if fixtures and rated == 0:
                print("  LABEL: coverage gap (fixtures present, zero rated players)")

        print("\nRe-running player_ratings snapshot for 48 WC squads...")
        from src.pipeline import bootstrap_tournament_pool  # noqa: E402

        pool = bootstrap_tournament_pool()
        stats = snapshot_player_ratings_for_pool(pool)
        print(f"  snapshot stats: {stats}")

        summary = get_player_ratings_snapshot_summary()
        _print_verdict(summary["snapshot_date"])

    except DatabaseError as exc:
        print(f"ERROR: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
