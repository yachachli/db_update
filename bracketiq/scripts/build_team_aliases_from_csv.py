"""
Build kenpom_aliases.json from a CSV of college team names so FanMatch and slate
always match (one canonical name per team, all variants as aliases).

CSV format (choose one):

  LONG (one row per alias; good for many variants per team):
    canonical,alias
    Gonzaga,Gonzaga
    Gonzaga,Gonzaga Bulldogs
    Fresno St.,Fresno St.
    Fresno St.,Fresno State
    Fresno St.,Fresno St Bulldogs

  WIDE (one row per team; first column = canonical, rest = aliases):
    canonical,alias_1,alias_2,alias_3
    Gonzaga,Gonzaga Bulldogs,Gonzaga University
    Fresno St.,Fresno State,Fresno St Bulldogs

  Headers are optional. If the first row looks like data (e.g. "Gonzaga,Gonzaga"),
  we treat column 0 as canonical and column 1 as alias (long format).

Output: data/kenpom_aliases.json with key "kenpom_aliases": { "Canonical": ["alias1", ...] }.
All matching (FanMatch, slate, odds) uses this file via team_name_resolver.resolve_to_canonical_kenpom().

Usage:
  python -m scripts.build_team_aliases_from_csv path/to/teams.csv
  python -m scripts.build_team_aliases_from_csv path/to/teams.csv --merge   # merge with existing aliases
  python -m scripts.build_team_aliases_from_csv path/to/teams.csv -o data/my_aliases.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

_BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BASE))


def _normalize_for_dedup(s: str) -> str:
    return (s or "").strip().lower()


def _detect_format(rows: list[list[str]]) -> str:
    """Return 'long' or 'wide' based on first data row."""
    if not rows:
        return "long"
    first = rows[0]
    if len(first) < 2:
        return "long"
    # If first row has header-like values (both columns same casing / look like titles), might be header
    a, b = (first[0] or "").strip(), (first[1] or "").strip()
    if a.lower() in ("canonical", "canonical_name", "kenpom") and b.lower() in ("alias", "aliases", "variant"):
        return "long"
    if a.lower() in ("canonical", "canonical_name") and len(first) > 2:
        return "wide"
    # Default: long (col0 = canonical, col1 = alias)
    return "long"


def _collect_long(rows: list[list[str]], skip_header: bool) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    start = 1 if skip_header else 0
    for row in rows[start:]:
        if len(row) < 2:
            continue
        canon = (row[0] or "").strip()
        alias = (row[1] or "").strip()
        if not canon:
            continue
        if canon not in out:
            out[canon] = []
        if alias and alias not in out[canon]:
            out[canon].append(alias)
    return out


def _collect_wide(rows: list[list[str]], skip_header: bool) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    start = 1 if skip_header else 0
    for row in rows[start:]:
        if not row:
            continue
        canon = (row[0] or "").strip()
        if not canon:
            continue
        aliases = [canon]  # include canonical itself
        for c in row[1:]:
            a = (c or "").strip()
            if a and a not in aliases:
                aliases.append(a)
        out[canon] = aliases
    return out


def load_csv(path: Path) -> list[list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def build_aliases(path: Path, merge: bool, out_path: Path) -> None:
    rows = load_csv(path)
    if not rows:
        print("CSV is empty.", file=sys.stderr)
        return

    # Detect header
    first = rows[0]
    skip_header = len(first) >= 2 and _normalize_for_dedup(first[0]) in (
        "canonical", "canonical_name", "kenpom"
    )
    fmt = _detect_format(rows)
    if fmt == "long":
        by_canon = _collect_long(rows, skip_header)
    else:
        by_canon = _collect_wide(rows, skip_header)

    # Ensure canonical is in its own alias list (for resolve_to_canonical_kenpom)
    for canon in list(by_canon.keys()):
        if canon not in by_canon[canon]:
            by_canon[canon].insert(0, canon)

    existing: dict = {}
    if merge and out_path.exists():
        try:
            with open(out_path, encoding="utf-8") as f:
                data = json.load(f)
            existing = data.get("kenpom_aliases", data) or {}
            if not isinstance(existing, dict):
                existing = {}
        except Exception as e:
            print(f"Could not load existing {out_path}: {e}. Writing new file.", file=sys.stderr)

    # Merge: same canonical -> merge alias lists (dedupe)
    for canon, aliases in by_canon.items():
        seen = {_normalize_for_dedup(a): a for a in (existing.get(canon) or [])}
        for a in aliases:
            k = _normalize_for_dedup(a)
            if k and k not in seen:
                seen[k] = a
        existing[canon] = list(seen.values())

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"kenpom_aliases": existing}, f, indent=2)

    print(f"Wrote {len(existing)} teams to {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build kenpom_aliases.json from a CSV so all name variants map to one canonical (no FanMatch match issues)."
    )
    parser.add_argument("csv_path", type=Path, help="Path to CSV (long: canonical,alias or wide: canonical,alias_1,alias_2,...)")
    parser.add_argument("--merge", action="store_true", help="Merge with existing kenpom_aliases.json instead of overwriting")
    parser.add_argument("-o", "--output", type=Path, default=None, help="Output JSON path (default: data/kenpom_aliases.json)")
    args = parser.parse_args()

    if not args.csv_path.exists():
        print(f"File not found: {args.csv_path}", file=sys.stderr)
        return 1

    out = args.output or (_BASE / "data" / "kenpom_aliases.json")
    build_aliases(args.csv_path, args.merge, out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
