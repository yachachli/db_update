"""
Export all unique team names from the live Odds API response to data/analysis/odds_team_names.txt.
Use to cross-reference with KenPom names and build a complete team_name_mapping.json.
Usage: py -m scripts.export_odds_team_names
Requires: ODDS_API_KEY in .env
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))


def main() -> int:
    from app.scrapers.odds_scraper import get_current_odds

    try:
        games = get_current_odds()
    except Exception as e:
        print(f"Failed to fetch odds: {e}", file=sys.stderr)
        return 1
    names = set()
    for g in games:
        if isinstance(g, dict):
            for key in ("home_team", "away_team"):
                t = g.get(key)
                if t and isinstance(t, str) and t.strip():
                    names.add(t.strip())
    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "odds_team_names.txt"
    with open(path, "w", encoding="utf-8") as f:
        for n in sorted(names):
            f.write(n + "\n")
    print(f"Exported {len(names)} Odds API team names to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
