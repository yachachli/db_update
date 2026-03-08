"""
Phase 1.5 Task 3A: Pull historical NCAAB spreads for every date in FanMatch dataset.
Incremental: only fetches dates not already in odds_2026.parquet (adds new dates only).
Saves to data/historical/odds_2026.parquet. 1-second delay between requests.
Usage: py -m scripts.collect_historical_odds [--test] [--full]
  --test = 3 dates only, print sample spreads
  --full = re-fetch all dates from FanMatch (ignore existing file).
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

# Load .env from backend directory so ODDS_API_KEY is set no matter where script is run from
_backend_root = Path(__file__).resolve().parent.parent
_dotenv_path = _backend_root / ".env"
if _dotenv_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_dotenv_path)

sys.path.insert(0, str(_backend_root))

from app.config import get_historical_dir, settings
from app.scrapers.odds_scraper import (
    get_historical_odds,
    parse_game_odds,
    odds_to_kenpom_name,
)


def main():
    parser = argparse.ArgumentParser(description="Collect historical NCAAB odds (incremental by default)")
    parser.add_argument("--test", action="store_true", help="Run 3 dates only and print sample spreads")
    parser.add_argument("--full", action="store_true", help="Re-fetch all dates from FanMatch (ignore existing odds file)")
    parser.add_argument("--debug-first", action="store_true", help="Print raw API spread structure for first game only")
    args = parser.parse_args()
    # Resolve historical dir: try config (cwd-relative), then backend/data/historical relative to this script
    hist_dir = get_historical_dir()
    if not hist_dir.is_absolute():
        hist_dir = Path.cwd() / hist_dir
    fm_path = hist_dir / "fanmatch_2026.parquet"
    if not fm_path.exists():
        backend_root = Path(__file__).resolve().parent.parent
        for candidate in ["data/historical", "app/data/historical", "./data/historical"]:
            cand = (backend_root / candidate).resolve()
            if (cand / "fanmatch_2026.parquet").exists():
                fm_path = cand / "fanmatch_2026.parquet"
                hist_dir = cand
                break
    if not fm_path.exists():
        print("fanmatch_2026.parquet not found. Run collect_historical_fanmatch first.")
        print(f"  Looked in: {get_historical_dir()}, {Path(__file__).resolve().parent.parent / 'data/historical'}")
        return 1
    df = pd.read_parquet(fm_path)
    if "fanmatch_date" not in df.columns:
        print("No fanmatch_date column.")
        return 1
    dates_from_fm = sorted(df["fanmatch_date"].dropna().unique())
    dates_from_fm = [str(d)[:10] for d in dates_from_fm]

    # Incremental: only fetch dates we don't already have in odds_2026.parquet (unless --full)
    out_path = hist_dir / "odds_2026.parquet"
    existing_df = None
    dates_we_have = set()
    if out_path.exists() and not args.full and not args.test:
        try:
            existing_df = pd.read_parquet(out_path)
            if "game_date" in existing_df.columns and len(existing_df) > 0:
                dates_we_have = set(existing_df["game_date"].astype(str).str[:10])
                print(f"Existing odds file: {len(existing_df)} rows, {len(dates_we_have)} dates. Will only fetch missing dates.")
        except Exception as e:
            print(f"Could not load existing odds parquet: {e}. Proceeding with full fetch.")
    dates = [d for d in dates_from_fm if d not in dates_we_have]
    if not dates and not args.test:
        print("All FanMatch dates already in odds_2026.parquet. Nothing to do.")
        return 0
    if args.test:
        dates = dates[:3] if len(dates) >= 3 else (dates_from_fm[:3] if dates_from_fm else dates)
        print(f"TEST MODE: fetching {len(dates)} dates only.")
    else:
        print(f"Fetching historical odds for {len(dates)} new dates (skipping {len(dates_we_have)} already in file). 1s delay...")
    rows = []
    for i, date_str in enumerate(dates):
        time.sleep(1)
        try:
            response = get_historical_odds(date_str)
        except Exception as e:
            print(f"  {date_str}: {e}")
            continue
        # Historical API returns { "data": [ ...events... ], "timestamp": ... }
        if i == 0 and args.test and isinstance(response, dict):
            print("\n--- First response (first 5000 chars) ---")
            print(json.dumps(response, indent=2)[:5000])
            print("---\n")
        games = response.get("data", []) if isinstance(response, dict) else response
        if not isinstance(games, list):
            games = []
        first_game_debug = args.debug_first and i == 0
        for g in games:
            if not isinstance(g, dict):
                continue
            if first_game_debug:
                home_team = g.get("home_team", "")
                away_team = g.get("away_team", "")
                print(f"\n{'='*60}")
                print(f"RAW GAME: {away_team} at {home_team}")
                print(f"Bookmakers: {len(g.get('bookmakers', []))}")
                for bk in g.get("bookmakers", []):
                    print(f"\n  Bookmaker: {bk.get('key', '?')}")
                    for mkt in bk.get("markets", []):
                        if mkt.get("key") == "spreads":
                            print("    Market: spreads")
                            for outcome in mkt.get("outcomes", []):
                                print(f"      {outcome.get('name')}: point={outcome.get('point')}, price={outcome.get('price')}")
                            for outcome in mkt.get("outcomes", []):
                                name = outcome.get("name", "")
                                if name == home_team:
                                    print(f"    -> Matched HOME '{home_team}': point={outcome.get('point')}")
                                elif name == away_team:
                                    print(f"    -> Matched AWAY '{away_team}': point={outcome.get('point')}")
                                else:
                                    print(f"    -> NO MATCH '{name}' (home='{home_team}', away='{away_team}')")
                print("=" * 60)
                first_game_debug = False
            parsed = parse_game_odds(g)
            if not parsed or parsed.get("consensus_spread") is None:
                continue
            home = parsed["home_team"]
            away = parsed["away_team"]
            rows.append({
                "game_date": date_str,
                "home_team": home,
                "away_team": away,
                "home_team_kenpom": odds_to_kenpom_name(home),
                "away_team_kenpom": odds_to_kenpom_name(away),
                "consensus_spread": parsed["consensus_spread"],
                "consensus_total": parsed.get("consensus_total"),
                "num_bookmakers": parsed.get("num_bookmakers", 0),
            })
        if args.test:
            print(f"\n=== {date_str} ===")
            for g in games[:5]:
                if not isinstance(g, dict):
                    continue
                p = parse_game_odds(g)
                if p and p.get("consensus_spread") is not None:
                    print(f"  {p['away_team']} @ {p['home_team']}: spread={p['consensus_spread']}, total={p.get('consensus_total')}")
        if (i + 1) % 20 == 0 and not args.test:
            print(f"  {i + 1}/{len(dates)} dates, {len(rows)} games")
    if not rows:
        if not args.test and existing_df is not None and len(existing_df) > 0:
            print("No new odds data collected. Existing file unchanged.")
        else:
            print("No odds data collected.")
        return 0
    if not args.test:
        new_df = pd.DataFrame(rows)
        if existing_df is not None and len(existing_df) > 0:
            consolidated = pd.concat([existing_df, new_df], ignore_index=True)
            consolidated.to_parquet(out_path, index=False)
            print(f"Added {len(new_df)} rows from {len(dates)} new dates. Total: {len(consolidated)} rows saved to {out_path}")
        else:
            new_df.to_parquet(out_path, index=False)
            print(f"Saved {len(new_df)} rows to {out_path}")
    else:
        print(f"\nTest: would save {len(rows)} rows. Run without --test to write parquet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
