"""
Build team_name_mapping.json from FanMatch + odds parquets so Odds API names
map to the same short names used in FanMatch/KenPom. Run after collect_historical_odds.
"""
import json
import sys
from pathlib import Path

import pandas as pd

_backend_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_backend_root))

from app.config import get_historical_dir
from app.services.schedule_service import parse_fanmatch_game


def _team_match(fm_name: str, odds_name: str) -> bool:
    if pd.isna(fm_name) or pd.isna(odds_name):
        return False
    a, b = str(fm_name).strip(), str(odds_name).strip()
    if a == b:
        return True
    if b.startswith(a + " "):
        return True
    if a.startswith(b + " "):
        return True
    return False


def _row_matches(home_fm: str, away_fm: str, home_odds: str, away_odds: str) -> bool:
    return (
        (_team_match(home_fm, home_odds) and _team_match(away_fm, away_odds))
        or (_team_match(home_fm, away_odds) and _team_match(away_fm, home_odds))
    )


def main():
    hist_dir = get_historical_dir()
    if not hist_dir.is_absolute():
        hist_dir = Path.cwd() / hist_dir
    fm_path = hist_dir / "fanmatch_2026.parquet"
    odds_path = hist_dir / "odds_2026.parquet"
    if not fm_path.exists() or not odds_path.exists():
        for candidate in ["data/historical", "app/data/historical"]:
            cand = (_backend_root / candidate).resolve()
            if (cand / "fanmatch_2026.parquet").exists() and (cand / "odds_2026.parquet").exists():
                hist_dir = cand
                fm_path = cand / "fanmatch_2026.parquet"
                odds_path = cand / "odds_2026.parquet"
                break
    if not fm_path.exists():
        print("fanmatch_2026.parquet not found.")
        return 1
    if not odds_path.exists():
        print("odds_2026.parquet not found. Run collect_historical_odds first.")
        return 1

    fm = pd.read_parquet(fm_path)
    odds = pd.read_parquet(odds_path)

    # 1) All FanMatch team names (from parsed Game + Winner/Loser)
    fm_teams = set()
    for _, r in fm.iterrows():
        g = r.get("Game")
        if pd.notna(g):
            p = parse_fanmatch_game(str(g))
            if p:
                fm_teams.add(p["home_team"].strip())
                fm_teams.add(p["away_team"].strip())
        for col in ("Winner", "Loser", "PredictedWinner", "PredictedLoser"):
            v = r.get(col)
            if pd.notna(v):
                fm_teams.add(str(v).strip())
    fm_teams.discard("")

    # 2) Infer from matched games: odds raw name -> FanMatch name
    odds_to_kenpom: dict[str, str] = {}
    for _, row in fm.iterrows():
        date_val = row.get("fanmatch_date")
        if pd.isna(date_val):
            continue
        date_str = str(date_val)[:10]
        game_str = row.get("Game")
        if pd.isna(game_str):
            continue
        parsed = parse_fanmatch_game(str(game_str))
        if not parsed:
            continue
        home_fm = parsed["home_team"]
        away_fm = parsed["away_team"]
        on_date = odds[odds["game_date"] == date_str]
        for _, o in on_date.iterrows():
            if not _row_matches(
                home_fm, away_fm,
                o.get("home_team_kenpom"), o.get("away_team_kenpom")
            ):
                continue
            raw_home = str(o.get("home_team", "")).strip()
            raw_away = str(o.get("away_team", "")).strip()
            if _team_match(home_fm, o.get("home_team_kenpom")):
                if raw_home and home_fm:
                    odds_to_kenpom[raw_home] = home_fm
                if raw_away and away_fm:
                    odds_to_kenpom[raw_away] = away_fm
            else:
                if raw_home and away_fm:
                    odds_to_kenpom[raw_home] = away_fm
                if raw_away and home_fm:
                    odds_to_kenpom[raw_away] = home_fm
            break

    # 3) Heuristic for unmapped Odds names: strip last word(s) and match to FM
    all_odds_raw = set(odds["home_team"].dropna().astype(str).str.strip()) | set(
        odds["away_team"].dropna().astype(str).str.strip()
    )
    for raw in all_odds_raw:
        if not raw or raw in odds_to_kenpom:
            continue
        parts = raw.split()
        # Try strip last word
        if len(parts) >= 2:
            short = " ".join(parts[:-1])
            if short in fm_teams:
                odds_to_kenpom[raw] = short
                continue
        if len(parts) >= 3:
            short2 = " ".join(parts[:-2])
            if short2 in fm_teams:
                odds_to_kenpom[raw] = short2
                continue
        # Try first token as school (e.g. "St." edge case: keep "St. John's" style)
        if parts and parts[0] in fm_teams and len(parts) == 1:
            odds_to_kenpom[raw] = parts[0]

    # 4) Normalize FanMatch-style variants (e.g. "Alabama St." vs "Alabama St")
    for odds_name, kenpom in list(odds_to_kenpom.items()):
        if kenpom in fm_teams:
            continue
        for fm_team in fm_teams:
            if fm_team.rstrip(".") == kenpom.rstrip(".") or (kenpom + "." == fm_team) or (fm_team + "." == kenpom):
                odds_to_kenpom[odds_name] = fm_team
                break

    # Merge with built-in: built-in takes precedence (use static map, not JSON-polluted one)
    from app.scrapers.odds_scraper import BUILTIN_ODDS_TO_KENPOM
    merged = dict(BUILTIN_ODDS_TO_KENPOM)
    for k, v in odds_to_kenpom.items():
        if k not in merged:
            merged[k] = v
    odds_to_kenpom = merged
    kenpom_to_odds = {v: k for k, v in odds_to_kenpom.items()}

    out_dir = _backend_root / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "team_name_mapping.json"
    payload = {"odds_to_kenpom": odds_to_kenpom, "kenpom_to_odds": kenpom_to_odds}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved {len(odds_to_kenpom)} odds->kenpom entries to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
