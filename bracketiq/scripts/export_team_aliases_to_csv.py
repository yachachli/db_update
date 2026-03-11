"""
Export data/kenpom_aliases.json and data/team_name_mapping.json (odds_to_kenpom)
into a single long-format CSV: canonical,alias.
Run from repo root: python -m scripts.export_team_aliases_to_csv
Output: data/team_aliases.csv
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))


def main() -> int:
    data_dir = _BASE / "data"
    out_path = data_dir / "team_aliases.csv"
    rows = []  # (canonical, alias)
    seen = set()  # (canonical_lower, alias_lower) for dedup

    # 1. kenpom_aliases.json
    aliases_path = data_dir / "kenpom_aliases.json"
    if aliases_path.exists():
        with open(aliases_path, encoding="utf-8") as f:
            data = json.load(f)
        kenpom = data.get("kenpom_aliases", data) or {}
        for canonical, aliases in kenpom.items():
            if not isinstance(aliases, list):
                continue
            c = (canonical or "").strip()
            if not c:
                continue
            for a in aliases:
                a = (a or "").strip()
                if not a:
                    continue
                key = (c.lower(), a.lower())
                if key not in seen:
                    seen.add(key)
                    rows.append((c, a))

    # 2. team_name_mapping.json odds_to_kenpom (Odds API name -> KenPom name)
    mapping_path = data_dir / "team_name_mapping.json"
    if mapping_path.exists():
        with open(mapping_path, encoding="utf-8") as f:
            data = json.load(f)
        odds_to_k = data.get("odds_to_kenpom", {}) or {}
        for odds_name, kenpom_name in odds_to_k.items():
            c = (kenpom_name or "").strip()
            a = (odds_name or "").strip()
            if not c or not a:
                continue
            key = (c.lower(), a.lower())
            if key not in seen:
                seen.add(key)
                rows.append((c, a))

    # Sort by canonical then alias for readability
    rows.sort(key=lambda x: (x[0].lower(), x[1].lower()))
    data_dir.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        import csv
        w = csv.writer(f)
        w.writerow(["canonical", "alias"])
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
