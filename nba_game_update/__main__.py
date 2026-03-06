"""Entry point: python -m nba_game_update"""

import os
import sys
from datetime import datetime, timedelta
from .pipeline import run_game_update, get_engine, fetch_and_upsert_games, detect_missing_dates
from .tank01_client import Tank01Client


def main():
    try:
        engine = get_engine()
        
        # Check for backfill environment variable (number of days to check)
        backfill_days = int(os.getenv("BACKFILL_DAYS", "0"))
        
        # Check for manual date range backfill
        backfill_start = os.getenv("BACKFILL_START")  # YYYYMMDD format
        backfill_end = os.getenv("BACKFILL_END")      # YYYYMMDD format
        
        if backfill_start and backfill_end:
            # Manual date range backfill
            print(f"Manual backfill mode: {backfill_start} to {backfill_end}")
            api_key = os.getenv("RAPIDAPI_KEY") or os.getenv("TANK01_API_KEY")
            if not api_key:
                raise RuntimeError("Set RAPIDAPI_KEY or TANK01_API_KEY")
            
            client = Tank01Client(api_key)
            start_dt = datetime.strptime(backfill_start, "%Y%m%d")
            end_dt = datetime.strptime(backfill_end, "%Y%m%d")
            
            current = start_dt
            total = 0
            dates_to_process = []
            while current <= end_dt:
                dates_to_process.append(current.strftime("%Y%m%d"))
                current += timedelta(days=1)
            
            print(f"Processing {len(dates_to_process)} dates...")
            import time
            for i, date_str in enumerate(dates_to_process, 1):
                total += fetch_and_upsert_games(engine, client, date_str)
                # Add extra delay between dates to avoid rate limiting
                if i < len(dates_to_process):
                    time.sleep(1.0)  # 1 second between dates
            
            print(f"\nBackfill complete: {total} games updated")
        else:
            # Normal daily update with optional automatic backfill
            run_game_update(engine, backfill_days=backfill_days)
            
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
