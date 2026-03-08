"""
Task 2: Historical FanMatch collection for 2025-26 season.
Scrapes FanMatch for each day from ~Nov 4, 2025 through today.
Incremental: only scrapes dates not already in fanmatch_2026.parquet (adds new games only).
10-second delay between date requests. Saves to data/historical/fanmatch_2026.parquet.
Run from backend dir: python -m scripts.collect_historical_fanmatch
  Use --full to re-scrape every date from season start (ignore existing file).
"""

import argparse
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Add backend to path so app.config is resolvable
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_historical_dir, settings
from app.scrapers.kenpom_scraper import get_kenpom_browser, get_fanmatch_for_date


FANMATCH_DELAY_SEC = 10
FANMATCH_JITTER = (0, 2)
SEASON_START = datetime(2025, 11, 4)


def _date_range(start: datetime, end: datetime):
    """Yield date strings YYYY-MM-DD from start through end (naive or aware)."""
    end_naive = end.replace(tzinfo=None) if end.tzinfo else end
    start_naive = start.replace(tzinfo=None) if start.tzinfo else start
    d = start_naive
    while d <= end_naive:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


# Save a checkpoint every N days so you can see progress and have partial data if interrupted
CHECKPOINT_EVERY_DAYS = 7
PROGRESS_FILE = "scrape_progress.txt"


def _write_progress(historical_dir: Path, total_days: int, days_done: int, total_rows: int, current_date: str, status: str = "running") -> None:
    path = historical_dir / PROGRESS_FILE
    with open(path, "w") as f:
        f.write(f"total_days={total_days}\n")
        f.write(f"days_done={days_done}\n")
        f.write(f"total_rows={total_rows}\n")
        f.write(f"current_date={current_date}\n")
        f.write(f"status={status}\n")
        if total_days > 0:
            f.write(f"percent={100.0 * days_done / total_days:.1f}\n")


def collect_fanmatch_2026(full_rescrape: bool = False, today_only: bool = False) -> None:
    end = datetime.now(timezone.utc)
    today_str = end.strftime("%Y-%m-%d")
    historical_dir = get_historical_dir()
    if not historical_dir.is_absolute():
        historical_dir = Path.cwd() / historical_dir
    out_path = historical_dir / "fanmatch_2026.parquet"
    partial_path = historical_dir / "fanmatch_2026_partial.parquet"
    progress_path = historical_dir / PROGRESS_FILE

    # Load existing data so we only scrape dates we don't have (unless --full)
    existing_df = None
    dates_we_have = set()
    if out_path.exists() and not full_rescrape:
        try:
            existing_df = pd.read_parquet(out_path)
            if "fanmatch_date" in existing_df.columns and len(existing_df) > 0:
                dates_we_have = set(existing_df["fanmatch_date"].astype(str).str[:10])
                if not today_only:
                    print(f"Existing file: {len(existing_df)} rows, {len(dates_we_have)} dates already collected. Will only add missing dates.")
        except Exception as e:
            print(f"Could not load existing parquet: {e}. Proceeding with full scrape.")

    browser = get_kenpom_browser()
    rows = []
    days_done = 0

    if today_only:
        if today_str in dates_we_have:
            print(f"Today ({today_str}) is already in fanmatch_2026.parquet. Nothing to do.")
            return
        dates_to_scrape = [today_str]
        total_days = 1
        print(f"Fetching KenPom FanMatch for today only ({today_str}). One request (~10s).")
    else:
        start_utc = SEASON_START.replace(tzinfo=timezone.utc) if SEASON_START.tzinfo is None else SEASON_START
        all_dates = list(_date_range(SEASON_START, end))
        dates_to_scrape = [d for d in all_dates if d not in dates_we_have]
        total_days = len(dates_to_scrape)
        if total_days == 0:
            print("All dates from season start through today are already in fanmatch_2026.parquet. Nothing to do.")
            _write_progress(historical_dir, len(all_dates), len(all_dates), len(existing_df) if existing_df is not None else 0, "end", "done")
            return
        print(f"Scraping FanMatch: {total_days} new dates (skipping {len(dates_we_have)} already in file). ~10s per date.")
    _write_progress(historical_dir, total_days, 0, 0, "start", "running")

    for date_str in dates_to_scrape:
        time.sleep(FANMATCH_DELAY_SEC + random.uniform(*FANMATCH_JITTER))
        fm = get_fanmatch_for_date(browser, date_str)
        days_done += 1
        total_rows = sum(len(r) for r in rows)
        _write_progress(historical_dir, total_days, days_done, total_rows, date_str, "running")
        if fm is None or fm.fm_df is None:
            if days_done % 10 == 0:
                print(f"  {date_str} — no games (total rows so far: {total_rows})")
            continue
        df = fm.fm_df.copy()
        df["fanmatch_date"] = date_str
        df["mean_abs_err_pred_mov"] = fm.mean_abs_err_pred_mov
        df["mean_abs_err_pred_total_score"] = fm.mean_abs_err_pred_total_score
        df["bias_pred_total_score"] = fm.bias_pred_total_score
        df["record_favs"] = fm.record_favs
        df["expected_record_favs"] = fm.expected_record_favs
        df["exact_mov"] = fm.exact_mov
        rows.append(df)
        n = len(df)
        total_rows = sum(len(r) for r in rows)
        print(f"  {date_str} — {n} games (total rows: {total_rows})")

        # Checkpoint every N days so you have partial data and can see progress
        if len(rows) > 0 and total_rows > 0 and days_done % CHECKPOINT_EVERY_DAYS == 0:
            consolidated = pd.concat(rows, ignore_index=True)
            consolidated.to_parquet(partial_path, index=False)
            print(f"  >> Checkpoint: saved {len(consolidated)} rows to {partial_path.name}")

    if not rows:
        _write_progress(historical_dir, total_days, days_done, len(existing_df) if existing_df is not None else 0, "end", "done_no_data")
        print("No new FanMatch data collected.")
        if existing_df is not None:
            print(f"Existing file unchanged ({len(existing_df)} rows).")
        return
    new_consolidated = pd.concat(rows, ignore_index=True)
    if existing_df is not None and len(existing_df) > 0:
        # Merge: keep existing dates, add new rows (same columns)
        consolidated = pd.concat([existing_df, new_consolidated], ignore_index=True)
        print(f"Added {len(new_consolidated)} rows from {days_done} new dates. Total rows: {len(consolidated)}")
    else:
        consolidated = new_consolidated
        print(f"Saved {len(consolidated)} rows from {days_done} dates.")
    consolidated.to_parquet(out_path, index=False)
    if partial_path.exists():
        partial_path.unlink()
    _write_progress(historical_dir, total_days, days_done, len(consolidated), "end", "done")
    print(f"Done. Saved {len(consolidated)} rows to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect historical FanMatch data (incremental by default)")
    parser.add_argument("--full", action="store_true", help="Re-scrape all dates from season start (ignore existing file)")
    parser.add_argument("--today-only", action="store_true", help="Fetch only today's KenPom FanMatch and merge into parquet (for sanity check)")
    args = parser.parse_args()
    collect_fanmatch_2026(full_rescrape=args.full, today_only=args.today_only)
