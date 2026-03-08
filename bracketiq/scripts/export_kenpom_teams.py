"""
BracketIQ — Export team names from the KenPom Pomeroy ratings cache.
Use to verify name matching: compare this list to Odds API names and update
team_name_mapping.json / kenpom_aliases.json so lookups are 100% correct.
Usage: py -m scripts.export_kenpom_teams
Output: data/analysis/kenpom_team_names.txt (and prints to stdout).
"""
from __future__ import annotations

import sys
from pathlib import Path

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))


def main() -> int:
    from app.config import get_cache_dir

    cache_dir = get_cache_dir()
    if not cache_dir.is_absolute():
        cache_dir = Path.cwd() / cache_dir
    files = list(cache_dir.glob("pomeroy_ratings_*.parquet"))
    if not files:
        print("No pomeroy_ratings_*.parquet found. Run KenPom scrape first.", file=sys.stderr)
        return 1

    import pandas as pd
    path = max(files, key=lambda p: p.stat().st_mtime)
    df = pd.read_parquet(path)
    name_col = "Team" if "Team" in df.columns else "team"
    if name_col not in df.columns:
        print(f"Columns: {df.columns.tolist()}", file=sys.stderr)
        return 1

    names = sorted(df[name_col].astype(str).str.strip().unique().tolist())
    out_dir = _backend_root / "data" / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "kenpom_team_names.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        for n in names:
            if n and n != "nan":
                f.write(n + "\n")
    print(f"Exported {len(names)} KenPom team names to {out_path}")
    for n in names[:50]:
        if n and n != "nan":
            print(f"  {n}")
    if len(names) > 50:
        print(f"  ... and {len(names) - 50} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
