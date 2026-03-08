"""
Print a readable summary of slate_today.json (run after slate_today, or on existing file).
Usage: py -m scripts.slate_summary
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
path = _backend_root / "data" / "analysis" / "slate_today.json"


def main() -> int:
    if not path.exists():
        print(f"Not found: {path}", file=sys.stderr)
        return 1
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    total = data.get("total_games", 0)
    print(f"\n=== Slate summary ({path.name}) ===\n")
    print(f"  Date: {data.get('date', '?')}")
    print(f"  Total games: {total}")
    sources = data.get("data_sources")
    if sources:
        print(f"  Spread/O/U: {sources.get('spread_and_ou', '')}")
        print(f"  Game winner (ML): {sources.get('game_winner_moneyline', '')}")

    sanity = data.get("sanity_check")
    if sanity:
        print(f"\n  Sanity check vs KenPom:")
        print(f"    Checked: {sanity.get('checked')}")
        if sanity.get("checked"):
            print(f"    Games compared: {sanity.get('games_compared')} / {sanity.get('games_on_slate')}")
            warnings = sanity.get("warnings") or []
            print(f"    Warnings (not adjacent): {len(warnings)}")
            for w in warnings[:10]:
                print(f"      - {w.get('away_team')} at {w.get('home_team')}: {w.get('note', '')[:60]}")
            if len(warnings) > 10:
                print(f"      ... and {len(warnings) - 10} more")
        else:
            print(f"    Reason: {sanity.get('reason', '')}")
    else:
        print("\n  Sanity check: not in file (run slate_today to include).")

    games = data.get("all_games") or data.get("games_sorted_by_spread_edge") or []
    if not games:
        print("\n  No games array found.")
        return 0

    by_conf = {}
    for g in games:
        c = g.get("spread_edge_confidence") or "?"
        by_conf[c] = by_conf.get(c, 0) + 1
    print("\n  Spread edge confidence:")
    for tier in ("NO_EDGE", "MILD", "STRONG", "HIGH_CONVICTION"):
        if tier in by_conf:
            print(f"    {tier}: {by_conf[tier]}")

    big_edge = [g for g in games if abs(g.get("spread_edge") or 0) > 15]
    big_margin = [g for g in games if abs(g.get("kenpom_predicted_margin_home_pov") or 0) > 25]
    print(f"\n  Review (|spread_edge| > 15): {len(big_edge)} games")
    for g in big_edge[:15]:
        rec = g.get("recency_adjustment_pts")
        rec_str = f", recency={rec}" if rec is not None else ""
        print(f"    {g.get('away_team_kenpom')} @ {g.get('home_team_kenpom')}: edge={g.get('spread_edge')}, margin={g.get('kenpom_predicted_margin_home_pov')}{rec_str}")
    if len(big_edge) > 15:
        print(f"    ... and {len(big_edge) - 15} more")
    if big_margin and len(big_margin) != len(big_edge):
        print(f"  Review (|margin| > 25): {len(big_margin)} games")

    print("\n  Sample — smaller edges (likely plausible):")
    small = [g for g in games if abs(g.get("spread_edge") or 0) <= 5 and abs(g.get("spread_edge") or 0) >= 0.5]
    for g in small[:8]:
        kp_fm = g.get("kenpom_fanmatch_margin_home_pov")
        adj = " (adjacent)" if g.get("sanity_adjacent") else (" (not adjacent)" if g.get("sanity_adjacent") is False else "")
        kp_str = f", KenPom FM margin={kp_fm}" if kp_fm is not None else ""
        print(f"    {g.get('away_team_kenpom')} @ {g.get('home_team_kenpom')}: edge={g.get('spread_edge')}, our margin={g.get('kenpom_predicted_margin_home_pov')}{kp_str}{adj}")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
