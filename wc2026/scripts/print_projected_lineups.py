"""Print projected XIs for selected teams (display-only; needs Neon + SportMonks).

Run from project root:

    py -3 scripts/print_projected_lineups.py
    py -3 scripts/print_projected_lineups.py Argentina Brazil Scotland
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json

from src.player_ratings import (
    build_team_player_display_for_code,
    resolve_team_code_for_id,
)
from src.sportmonks_client import SportmonksClient

_ROSTER_PATH = _ROOT / "data" / "wc2026_teams.json"
_DEFAULT_NAMES = (
    "Argentina",
    "Brazil",
    "Scotland",
    "Jordan",
    "United States",
    "Mexico",
    "Canada",
    "France",
    "England",
    "Germany",
    "Egypt",
    "Senegal",
    "Korea Republic",
)


def _load_roster() -> dict[int, str]:
    data = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    return {
        int(row["sportmonks_team_id"]): str(row["search_name"])
        for row in data.get("teams", [])
        if row.get("sportmonks_team_id")
    }


def _resolve_targets(argv: list[str]) -> list[tuple[int, str]]:
    roster = _load_roster()
    name_to_id = {name.lower(): tid for tid, name in roster.items()}
    if not argv:
        targets: list[tuple[int, str]] = []
        for name in _DEFAULT_NAMES:
            tid = name_to_id.get(name.lower())
            if tid is not None:
                targets.append((tid, roster[tid]))
        return targets
    targets = []
    for arg in argv:
        key = arg.lower()
        if key.isdigit():
            tid = int(key)
            name = roster.get(tid, f"team_{tid}")
            targets.append((tid, name))
        elif key in name_to_id:
            tid = name_to_id[key]
            targets.append((tid, roster[tid]))
        else:
            print(f"WARNING: unknown team '{arg}', skipping")
    return targets


def _fmt_player(row: dict) -> str:
    mins = row.get("minutes_share")
    mins_s = f"{mins:.0%}" if mins is not None else "n/a"
    rating = row.get("avg_rating")
    rating_s = f"{rating:.2f}" if rating is not None else "n/a"
    return (
        f"  #{row['squad_no']:>2} {row['position']:<3} "
        f"{row['player_name']:<28} "
        f"rating={rating_s}  mins={mins_s}  "
        f"({row.get('matches_counted', '?')} matches)"
    )


def _print_team(team_id: int, team_name: str, block: dict) -> None:
    print("=" * 78)
    print(f"{team_name} (id={team_id})  status={block.get('status', '?')}")
    print("-" * 78)
    xi = block.get("projected_xi") or []
    if not xi:
        print("  (no projected XI — check squad/id_map/ratings coverage)")
        squad = block.get("squad") or []
        if squad:
            print(f"  squad rows in Neon: {len(squad)}")
        return

    print("PROJECTED XI (4-3-3):")
    for row in xi:
        print(_fmt_player(row))

    bench = block.get("bench") or []
    if bench:
        print()
        print(f"BENCH ({len(bench)} rated subs):")
        for row in bench[:8]:
            print(_fmt_player(row))
        if len(bench) > 8:
            print(f"  ... +{len(bench) - 8} more")
    print()


def main() -> int:
    targets = _resolve_targets(sys.argv[1:])
    if not targets:
        print("No teams to display.")
        return 1

    client = SportmonksClient()
    for team_id, team_name in targets:
        team_code = resolve_team_code_for_id(team_id)
        if not team_code:
            print(f"WARNING: no FIFA code in Neon for {team_name} ({team_id})")
            continue
        block = build_team_player_display_for_code(team_code, team_id, client)
        _print_team(team_id, f"{team_name} [{team_code}]", block)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
