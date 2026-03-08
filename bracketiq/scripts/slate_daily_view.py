"""
BracketIQ — Clean daily slate view: games, Vegas vs model, and best value plays.
Reads from slate_today.json (run slate_today first to refresh).
Usage: py -m scripts.slate_daily_view
       py -m scripts.slate_daily_view --top 15
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
DEFAULT_SLATE_PATH = _backend_root / "data" / "analysis" / "slate_today.json"
TOP_N_DEFAULT = 10


def _pct(x):
    if x is None:
        return " — "
    return f"{100 * float(x):.1f}%"


def _num(x, decimals=1):
    if x is None:
        return " — "
    return f"{float(x):.{decimals}f}"


def _spread_str(x):
    if x is None:
        return " — "
    v = float(x)
    return f"{v:+.1f}" if v != 0 else "0.0"


def load_slate(path: Path):
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Daily slate view with best value plays")
    parser.add_argument("--slate", type=Path, default=DEFAULT_SLATE_PATH, help="Path to slate_today.json")
    parser.add_argument("--top", type=int, default=TOP_N_DEFAULT, help="Number of top value plays per category (default 10)")
    args = parser.parse_args()

    data = load_slate(args.slate)
    if not data:
        print(f"Slate file not found: {args.slate}")
        print("Run first:  py -m scripts.slate_today")
        return 1

    games = data.get("all_games") or data.get("games_sorted_by_spread_edge") or []
    if not games:
        print("No games in slate.")
        return 0

    date = data.get("date", "today")
    total = data.get("total_games", len(games))
    top_n = max(1, min(args.top, 25))

    # ---------- Header ----------
    print()
    print("=" * 78)
    print(f"  BRACKETIQ DAILY SLATE — {date.upper()}  ({total} games)")
    print("=" * 78)
    sources = data.get("data_sources", {})
    if sources:
        print(f"  Spread/O/U: {sources.get('spread_and_ou', '')}")
        print(f"  Game winner (ML): {sources.get('game_winner_moneyline', '')}")
    print()

    # ---------- Best value plays ----------
    print("-" * 78)
    print("  BEST VALUE PLAYS")
    print("-" * 78)

    # Spread: sort by abs(edge), show top N with clear pick
    with_spread = [g for g in games if g.get("spread_edge") is not None]
    by_spread = sorted(with_spread, key=lambda g: -abs(g["spread_edge"]))[:top_n]
    print("\n  SPREAD (model vs Vegas) — top value by |edge|:")
    for i, g in enumerate(by_spread, 1):
        edge = g.get("spread_edge") or 0
        vegas = g.get("vegas_spread_home_pov")
        model = g.get("kenpom_predicted_margin_home_pov")
        away = g.get("away_team_kenpom") or g.get("away_team", "?")
        home = g.get("home_team_kenpom") or g.get("home_team", "?")
        if edge > 0:
            pick = f"HOME ({home})"
        else:
            pick = f"AWAY ({away})"
        conf = g.get("spread_edge_confidence", "")
        rate = g.get("historical_cover_rate", "")
        print(f"    {i:2}. {away} @ {home}")
        print(f"        Vegas: {_spread_str(vegas)} (home)  |  Model: {_spread_str(model)}  →  Edge: {_spread_str(edge)}  |  Pick: {pick}  ({conf}, {rate})")

    # Moneyline: sort by abs(moneyline_edge)
    with_ml = [g for g in games if g.get("moneyline_edge") is not None]
    by_ml = sorted(with_ml, key=lambda g: -abs(g["moneyline_edge"]))[:top_n]
    print("\n  MONEYLINE (our model vs Vegas) — top value by |edge|:")
    for i, g in enumerate(by_ml, 1):
        edge = g.get("moneyline_edge") or 0
        v_home = g.get("vegas_implied_prob_home")
        m_home = g.get("model_win_prob_home") or g.get("kenpom_win_prob_home")
        away = g.get("away_team_kenpom") or g.get("away_team", "?")
        home = g.get("home_team_kenpom") or g.get("home_team", "?")
        if edge > 0:
            pick = f"HOME ({home})"
        else:
            pick = f"AWAY ({away})"
        print(f"    {i:2}. {away} @ {home}")
        print(f"        Vegas ML home: {_pct(v_home)}  |  Model win% home: {_pct(m_home)}  →  Edge: {_pct(edge)}  |  Pick: {pick}")

    # O/U: sort by abs(ou_edge)
    with_ou = [g for g in games if g.get("over_under_edge") is not None]
    by_ou = sorted(with_ou, key=lambda g: -abs(g["over_under_edge"]))[:top_n]
    print("\n  OVER/UNDER — top value by |edge|:")
    for i, g in enumerate(by_ou, 1):
        edge = g.get("over_under_edge") or 0
        vegas_t = g.get("vegas_total")
        model_t = g.get("kenpom_predicted_total")
        away = g.get("away_team_kenpom") or g.get("away_team", "?")
        home = g.get("home_team_kenpom") or g.get("home_team", "?")
        if edge > 0:
            pick = "OVER"
        else:
            pick = "UNDER"
        print(f"    {i:2}. {away} @ {home}")
        print(f"        Vegas total: {_num(vegas_t)}  |  Model total: {_num(model_t)}  →  Edge: {_num(edge, 2)} pts  |  Pick: {pick}")

    # ---------- Full slate table ----------
    print()
    print("-" * 78)
    print("  FULL SLATE — Vegas vs Model")
    print("-" * 78)
    print(f"  {'Matchup':<42} {'Vegas':<18} {'Model':<18} {'Edges (S / ML / O-U)'}")
    print("-" * 78)

    spread_picks = {(g.get("away_team_kenpom"), g.get("home_team_kenpom")) for g in by_spread}
    ml_picks = {(g.get("away_team_kenpom"), g.get("home_team_kenpom")) for g in by_ml}
    ou_picks = {(g.get("away_team_kenpom"), g.get("home_team_kenpom")) for g in by_ou}

    for g in games:
        away = g.get("away_team_kenpom") or g.get("away_team", "?")
        home = g.get("home_team_kenpom") or g.get("home_team", "?")
        matchup = f"{away} @ {home}"[:40]
        vegas_s = _spread_str(g.get("vegas_spread_home_pov"))
        vegas_t = _num(g.get("vegas_total"))
        vegas_ml = _pct(g.get("vegas_implied_prob_home"))
        vegas_str = f"{vegas_s}  {vegas_t}  {vegas_ml}"[:17]
        model_s = _spread_str(g.get("kenpom_predicted_margin_home_pov"))
        model_t = _num(g.get("kenpom_predicted_total"))
        model_ml = _pct(g.get("model_win_prob_home") or g.get("kenpom_win_prob_home"))
        model_str = f"{model_s}  {model_t}  {model_ml}"[:17]
        se = g.get("spread_edge")
        me = g.get("moneyline_edge")
        oe = g.get("over_under_edge")
        edge_s = _spread_str(se) if se is not None else " — "
        edge_ml = _pct(me) if me is not None else " — "
        edge_ou = _num(oe, 2) if oe is not None else " — "
        edges_str = f"{edge_s}  {edge_ml}  {edge_ou}"
        tags = []
        key = (g.get("away_team_kenpom"), g.get("home_team_kenpom"))
        if key in spread_picks:
            tags.append("SPREAD")
        if key in ml_picks:
            tags.append("ML")
        if key in ou_picks:
            tags.append("O/U")
        tag_str = f"  ← {', '.join(tags)}" if tags else ""
        print(f"  {matchup:<42} {vegas_str:<18} {model_str:<18} {edges_str}{tag_str}")

    print("-" * 78)
    print("  Legend: Vegas = spread (home POV), total, home ML%  |  Model = pred margin, pred total, model win%")
    print("  Edges: spread edge | ML edge | O/U edge (pts).  ← SPREAD/ML/O/U = top value play in that category.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
