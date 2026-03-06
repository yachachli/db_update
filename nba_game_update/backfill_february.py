#!/usr/bin/env python3
"""
One-time backfill script for all of February 2025.
Fetches and upserts NBA game results for all dates in February.
"""

import os
import sys
from datetime import datetime, timedelta

from .pipeline import get_engine, fetch_and_upsert_games
from .tank01_client import Tank01Client


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
    
    print(f"Backfilling NBA game results for February 2025")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print()
    
    engine = get_engine()
    client = Tank01Client(api_key)
    
    total_games = 0
    dates_processed = 0
    dates_with_games = 0
    
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
            print(f"  ERROR processing {date_display}: {e}", file=sys.stderr)
        
        current += timedelta(days=1)
    
    print()
    print("=" * 60)
    print(f"Backfill complete!")
    print(f"  Dates processed: {dates_processed}")
    print(f"  Dates with games: {dates_with_games}")
    print(f"  Total games upserted: {total_games}")
    print("=" * 60)


if __name__ == "__main__":
    main()
