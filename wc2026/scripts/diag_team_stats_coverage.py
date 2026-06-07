"""Diagnose SportMonks stat coverage for all pool teams (read-only).

Reports which teams have zero-stat qualifier windows (degenerate rating risk),
what fixtures were found, and whether raw xgfixture payloads carry data.

    py -3 scripts/diag_team_stats_coverage.py
    py -3 scripts/diag_team_stats_coverage.py --team-id 18559
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.aggregation import compute_team_rating  # noqa: E402
from src.database import get_fifa_code_for_team_id, get_wc2026_squad_team_codes  # noqa: E402
from src.pipeline import bootstrap_tournament_pool  # noqa: E402
from src.sportmonks_client import SportmonksClient, QUALIFIER_LEAGUE_IDS  # noqa: E402
from src.sportmonks_parser import parse_fixture_to_match_stats, STAT_TYPE_IDS  # noqa: E402

_LEAGUES = json.loads((_PROJECT_ROOT / "data" / "leagues_catalog.json").read_text())
_LEAGUE_NAMES = {int(r["id"]): r["name"] for r in _LEAGUES if r.get("id")}


def _has_core_stats(match) -> bool:
    return not (
        match.goals_scored == 0
        and match.goals_conceded == 0
        and match.xg_created == 0
        and match.xg_conceded == 0
    )


def _xgfixture_summary(fixture: dict, team_id: int) -> dict:
    entries = fixture.get("xgfixture") or []
    if not isinstance(entries, list):
        return {"entries": 0, "types_for_team": []}
    types = sorted({
        int(e.get("type_id"))
        for e in entries
        if isinstance(e, dict) and e.get("participant_id") == team_id
    })
    return {"entries": len(entries), "types_for_team": types}


def _inspect_team(pool, client: SportmonksClient, team_id: int) -> dict:
    team = pool.teams.get(team_id)
    if not team:
        return {"team_id": team_id, "error": "not in pool.teams"}

    code = get_fifa_code_for_team_id(team_id) or "?"
    host = team_id in pool.host_ratings
    matches = pool.matches_by_team.get(team_id, [])
    usable = [m for m in matches if _has_core_stats(m)]
    degenerate = False
    rating = None
    if host:
        rating = pool.host_ratings[team_id]
    elif matches:
        rating = compute_team_rating(team, matches, pool.baseline)
        degenerate = rating.attack_final == 0.0 and rating.defense_final == 0.0

    raw_fixtures = client.get_fixtures_for_team(team_id, limit=5)
    fixture_rows = []
    for fx in raw_fixtures:
        lid = fx.get("league_id")
        parsed = None
        try:
            opp_pts = 1200.0
            parsed = parse_fixture_to_match_stats(fx, team_id, opp_pts)
        except Exception as exc:
            parsed = exc
        fixture_rows.append({
            "fixture_id": fx.get("id"),
            "date": str(fx.get("starting_at", ""))[:10],
            "league_id": lid,
            "league": _LEAGUE_NAMES.get(int(lid) if lid else 0, "?"),
            "participants": [
                p.get("name") for p in (fx.get("participants") or [])
                if isinstance(p, dict)
            ],
            "xgfixture": _xgfixture_summary(fx, team_id),
            "parsed_core": (
                None if isinstance(parsed, Exception) else {
                    "goals": parsed.goals_scored,
                    "xg": parsed.xg_created,
                    "xgc": parsed.xg_conceded,
                    "usable": _has_core_stats(parsed),
                }
            ),
            "parse_error": str(parsed) if isinstance(parsed, Exception) else None,
        })

    # Unfiltered sample: same window, no league filter
    unfiltered = client.get(
        f"fixtures/between/2022-06-06/2026-06-06/{team_id}",
        params={
            "include": "xGFixture;participants;scores;state;venue",
            "per_page": 10,
        },
    ).get("data") or []
    unfiltered.sort(
        key=lambda f: f.get("starting_at_timestamp") or 0, reverse=True
    )
    other_leagues = []
    for fx in unfiltered[:10]:
        lid = fx.get("league_id")
        if lid not in QUALIFIER_LEAGUE_IDS:
            summ = _xgfixture_summary(fx, team_id)
            other_leagues.append({
                "fixture_id": fx.get("id"),
                "league_id": lid,
                "league": _LEAGUE_NAMES.get(int(lid) if lid else 0, "?"),
                "xg_entries": summ["entries"],
                "types": summ["types_for_team"],
            })

    return {
        "team_id": team_id,
        "code": code,
        "name": team.name,
        "host": host,
        "pool_matches": len(matches),
        "usable_matches": len(usable),
        "degenerate": degenerate,
        "attack_final": None if rating is None else round(rating.attack_final, 4),
        "defense_final": None if rating is None else round(rating.defense_final, 4),
        "qualifier_fixtures": fixture_rows,
        "recent_non_qualifier_leagues": other_leagues[:5],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team-id", type=int, default=None)
    args = parser.parse_args()

    print("=" * 78)
    print("DIAG: TEAM STATS COVERAGE")
    print("=" * 78)

    pool = bootstrap_tournament_pool()
    client = SportmonksClient()

    if args.team_id:
        team_ids = [args.team_id]
    else:
        squad_codes = set(get_wc2026_squad_team_codes())
        team_ids = sorted(
            tid for tid in pool.matches_by_team
            if (get_fifa_code_for_team_id(tid) or "") in squad_codes
            or tid in pool.host_ratings
        )
        # include predictable roster teams
        team_ids = sorted(set(pool.matches_by_team) | set(pool.host_ratings))

    degenerate_teams: list[dict] = []
    thin_teams: list[dict] = []

    for tid in team_ids:
        row = _inspect_team(pool, client, tid)
        if row.get("host"):
            continue
        if row.get("degenerate"):
            degenerate_teams.append(row)
        elif row.get("usable_matches", 0) < 3:
            thin_teams.append(row)

    print(f"\nPool teams inspected: {len(team_ids)}")
    print(f"Degenerate (attack=defense=0): {len(degenerate_teams)}")
    print(f"Thin usable window (<3 matches with stats): {len(thin_teams)}")

    if degenerate_teams:
        print("\n--- DEGENERATE TEAMS ---")
        for row in degenerate_teams:
            print(
                f"\n{row['code']} {row['name']} (id={row['team_id']}) "
                f"pool={row['pool_matches']} usable={row['usable_matches']}"
            )
            for fx in row["qualifier_fixtures"]:
                print(f"  QF {fx['fixture_id']} {fx['date']} {fx['league']}: "
                      f"xgfixture={fx['xgfixture']} parsed={fx['parsed_core']}")
            if row["recent_non_qualifier_leagues"]:
                print("  Non-qualifier fixtures with stats potential:")
                for fx in row["recent_non_qualifier_leagues"]:
                    if fx["xg_entries"]:
                        print(f"    {fx['fixture_id']} league={fx['league']} "
                              f"types={fx['types']}")

    if args.team_id:
        print(json.dumps(_inspect_team(pool, client, args.team_id), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
