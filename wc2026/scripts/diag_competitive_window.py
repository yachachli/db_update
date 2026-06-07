"""Phase 4.1b Part 1: validate player-ratings fixture window is competitive-only.

No DB writes, no src changes. Compares fetch_lineup_fixtures_for_team() against
the prediction pool's qualifier match_ids for sample teams.

Run from the project root:

    py -3 scripts/diag_competitive_window.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src import config  # noqa: E402
from src.pipeline import bootstrap_tournament_pool  # noqa: E402
from src.player_ratings import fetch_lineup_fixtures_for_team  # noqa: E402
from src.sportmonks_client import (  # noqa: E402
    QUALIFIER_LEAGUE_IDS,
    SportmonksClient,
)

_LEAGUES_CATALOG = _PROJECT_ROOT / "data" / "leagues_catalog.json"

SAMPLE_TEAMS: list[dict[str, Any]] = [
    {"team_code": "USA", "search_name": "United States", "team_id": 18571},
    {"team_code": "ESP", "search_name": "Spain", "team_id": 18710},
    {"team_code": "ARG", "search_name": "Argentina", "team_id": 18644},
    {"team_code": "KOR", "search_name": "Korea Republic", "team_id": 18567},
]

ALLOWED_LEAGUE_IDS = frozenset(QUALIFIER_LEAGUE_IDS)


def _load_league_names() -> dict[int, str]:
    if not _LEAGUES_CATALOG.exists():
        return {}
    catalog = json.loads(_LEAGUES_CATALOG.read_text(encoding="utf-8"))
    return {int(row["id"]): str(row["name"]) for row in catalog if row.get("id")}


def _fixture_date(fixture: dict[str, Any]) -> str:
    for key in ("starting_at", "starting_at_date"):
        if fixture.get(key):
            return str(fixture[key])[:10]
    ts = fixture.get("starting_at_timestamp")
    if ts:
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
    return "?"


def _participants(fixture: dict[str, Any]) -> str:
    parts = fixture.get("participants") or []
    if not isinstance(parts, list):
        return "?"
    names = []
    for p in parts:
        if isinstance(p, dict):
            names.append(str(p.get("name", "?")))
    return " vs ".join(names) if names else "?"


def _pool_match_ids(pool, team_id: int) -> list[int]:
    matches = pool.matches_by_team.get(team_id, [])
    return [int(m.match_id) for m in matches]


def _summaries_for_team(client: SportmonksClient, team_id: int) -> list[dict[str, Any]]:
    """Same listing path fetch_lineup_fixtures_for_team uses before lineup fetch."""
    return client.get_fixtures_for_team(
        team_id, limit=config.PLAYER_RATINGS_MATCH_WINDOW
    )


def _analyze_team(
    client: SportmonksClient,
    pool,
    league_names: dict[int, str],
    team: dict[str, Any],
) -> dict[str, Any]:
    team_id = int(team["team_id"])
    team_code = str(team["team_code"])
    search_name = str(team["search_name"])

    summaries = _summaries_for_team(client, team_id)
    lineup_fixtures = fetch_lineup_fixtures_for_team(client, team_id)

    leaks: list[dict[str, Any]] = []
    fixture_rows: list[dict[str, Any]] = []

    for summary in summaries:
        fixture_id = int(summary["id"])
        league_id = summary.get("league_id")
        league_id_int = int(league_id) if league_id is not None else None
        league_name = league_names.get(league_id_int, "UNKNOWN") if league_id_int else "MISSING"
        is_allowed = league_id_int in ALLOWED_LEAGUE_IDS if league_id_int else False
        row = {
            "fixture_id": fixture_id,
            "date": _fixture_date(summary),
            "league_id": league_id_int,
            "league_name": league_name,
            "participants": _participants(summary),
            "allowed": is_allowed,
        }
        fixture_rows.append(row)
        if not is_allowed:
            leaks.append(row)

    display_ids = [int(f["id"]) for f in lineup_fixtures if f.get("id") is not None]
    summary_ids = [int(s["id"]) for s in summaries if s.get("id") is not None]
    pool_ids = _pool_match_ids(pool, team_id)

    overlap_pool = sorted(set(summary_ids) & set(pool_ids))
    only_display = sorted(set(summary_ids) - set(pool_ids))
    only_pool = sorted(set(pool_ids) - set(summary_ids))

    verdict = "COMPETITIVE-ONLY" if not leaks else "LEAK DETECTED"

    return {
        "team_code": team_code,
        "search_name": search_name,
        "team_id": team_id,
        "fixture_rows": fixture_rows,
        "leaks": leaks,
        "summary_ids": summary_ids,
        "display_ids": display_ids,
        "pool_ids": pool_ids,
        "overlap_pool": overlap_pool,
        "only_display": only_display,
        "only_pool": only_pool,
        "verdict": verdict,
        "pool_empty": not pool_ids,
    }


def main() -> int:
    print("=" * 78)
    print("PHASE 4.1b PART 1 — COMPETITIVE WINDOW VALIDATION (no DB writes)")
    print("=" * 78)
    print(f"Allowed league_ids: {sorted(ALLOWED_LEAGUE_IDS)}")
    print(f"Window size: {config.PLAYER_RATINGS_MATCH_WINDOW}")

    league_names = _load_league_names()
    client = SportmonksClient()

    print("\nBootstrapping prediction pool for cross-check...")
    try:
        pool = bootstrap_tournament_pool()
    except Exception as exc:
        print(f"ERROR: pool bootstrap failed: {exc}")
        return 1

    results: list[dict[str, Any]] = []
    any_leak = False

    for team in SAMPLE_TEAMS:
        analysis = _analyze_team(client, pool, league_names, team)
        results.append(analysis)
        if analysis["verdict"] == "LEAK DETECTED":
            any_leak = True

        print("\n" + "=" * 78)
        print(f"TEAM {analysis['team_code']} ({analysis['search_name']})  "
              f"team_id={analysis['team_id']}")
        print("=" * 78)
        print("\nfetch_lineup_fixtures_for_team window (via get_fixtures_for_team):")
        if not analysis["fixture_rows"]:
            print("  (no fixtures returned)")
        for row in analysis["fixture_rows"]:
            flag = "OK" if row["allowed"] else "LEAK"
            print(
                f"  [{flag}] id={row['fixture_id']}  date={row['date']}  "
                f"league_id={row['league_id']}  {row['league_name']!r}  "
                f"{row['participants']}"
            )

        if analysis["leaks"]:
            print("\n  *** LEAK WARNING — league_id NOT in allowed competitive set:")
            for row in analysis["leaks"]:
                print(
                    f"      id={row['fixture_id']} league_id={row['league_id']} "
                    f"{row['league_name']!r} ({row['participants']})"
                )

        print("\nPool cross-check (qualifier match_ids from bootstrap_tournament_pool):")
        if analysis["pool_empty"]:
            print("  pool: (empty — host or no parsed qualifier matches)")
        else:
            print(f"  pool match_ids:     {analysis['pool_ids']}")
        print(f"  display match_ids:  {analysis['summary_ids']}")
        print(f"  lineup fixtures fetched: {len(analysis['display_ids'])}")
        print(f"  overlap:            {analysis['overlap_pool']}")
        if analysis["only_display"]:
            print(f"  only in display:    {analysis['only_display']}")
        if analysis["only_pool"]:
            print(f"  only in pool:        {analysis['only_pool']}")

        print(f"\n  VERDICT: {analysis['verdict']}")

    print("\n" + "=" * 78)
    print("FIELD-WIDE SUMMARY")
    print("=" * 78)
    for analysis in results:
        leak_note = (
            f"LEAK ({len(analysis['leaks'])})"
            if analysis["leaks"]
            else "clean"
        )
        print(
            f"  {analysis['team_code']:<4} {analysis['verdict']:<20} "
            f"fixtures={len(analysis['fixture_rows'])}  "
            f"pool={len(analysis['pool_ids'])}  overlap={len(analysis['overlap_pool'])}  "
            f"{leak_note}"
        )

    print("\n" + "=" * 78)
    if any_leak:
        print("STOP — LEAK DETECTED. Fix fetch_lineup_fixtures_for_team filter before Part 2.")
        print("Apply the same competitive-match filter the prediction pool uses.")
        return 2

    print("STOP — Part 1 clean. All sample teams COMPETITIVE-ONLY.")
    print("Safe to proceed to Part 2 (player_ratings_history + cron persist).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
