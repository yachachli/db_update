#!/usr/bin/env python3
"""
One-time backfill script for all of February 2025.
Fetches and upserts NBA game results for all dates in February.

Usage:
    python backfill_february_games.py

Requires environment variables:
    RAPIDAPI_KEY or TANK01_API_KEY
    DB_URL or (DB_USER, DB_PASS, DB_HOST, DB_NAME)
"""

import os
import sys
from datetime import datetime, timedelta

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nba_game_update.pipeline import get_engine, fetch_and_upsert_games
from nba_game_update.tank01_client import Tank01Client


def main():
    """Backfill all dates in February 2025."""
    # Get API key
    api_key = os.getenv("RAPIDAPI_KEY") or os.getenv("TANK01_API_KEY")
    if not api_key:
        print("ERROR: Set RAPIDAPI_KEY or TANK01_API_KEY environment variable", file=sys.stderr)
        sys.exit(1)
    
    # February 2025: Feb 1 to Feb 28
    start_date = datetime(2025, 2, 1)
    end_date = datetime(2025, 2, 28)
    
    print("=" * 60)
    print("NBA Game Results Backfill - February 2025")
    print("=" * 60)
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print()
    
    try:
        engine = get_engine()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}", file=sys.stderr)
        print("Make sure DB_URL or DB_USER/DB_PASS/DB_HOST/DB_NAME are set", file=sys.stderr)
        sys.exit(1)
    
    client = Tank01Client(api_key)
    
    total_games = 0
    dates_processed = 0
    dates_with_games = 0
    errors = []
    
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y%m%d")
        date_display = current.strftime("%Y-%m-%d")
        
        try:
            games_count = fetch_and_upsert_games(engine, client, date_str)
            total_games += games_count
            dates_processed += 1
            if games_count > 0:
                dates_with_games += 1
        except Exception as e:
            error_msg = f"{date_display}: {e}"
            errors.append(error_msg)
            print(f"  ERROR processing {date_display}: {e}", file=sys.stderr)
        
        current += timedelta(days=1)
    
    print()
    print("=" * 60)
    print("Backfill Summary")
    print("=" * 60)
    print(f"  Dates processed: {dates_processed}/28")
    print(f"  Dates with games: {dates_with_games}")
    print(f"  Total games upserted: {total_games}")
    if errors:
        print(f"  Errors encountered: {len(errors)}")
        for err in errors:
            print(f"    - {err}")
    print("=" * 60)
    
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
