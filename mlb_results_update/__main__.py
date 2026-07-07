"""Entry point: python -m mlb_results_update

Modes (chosen by env vars, first match wins):
  - Manual date range:  BACKFILL_START=YYYYMMDD + BACKFILL_END=YYYYMMDD
  - Auto rolling:       BACKFILL_DAYS=N  (re-runs yesterday + last N days)
  - Single date:        TARGET_DATE=YYYY-MM-DD
  - Default:            yesterday (UTC date - 1)
"""
import os
import sys
import time
from datetime import date, datetime, timedelta

from .pipeline import (
    get_engine,
    sync_results_for_date,
    log_row_counts,
)


def _to_iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _default_target() -> date:
    """Default = yesterday (UTC). At 9 UTC = 5am ET, this is the day that just ended."""
    return date.today() - timedelta(days=1)


def main() -> None:
    try:
        engine = get_engine()

        backfill_start = os.getenv("BACKFILL_START")
        backfill_end = os.getenv("BACKFILL_END")
        backfill_days = int(os.getenv("BACKFILL_DAYS", "0"))
        target_date = os.getenv("TARGET_DATE")

        if backfill_start and backfill_end:
            start = _parse_yyyymmdd(backfill_start)
            end = _parse_yyyymmdd(backfill_end)
            print(f"Manual backfill mode: {start} to {end}")
            dates = []
            cur = start
            while cur <= end:
                dates.append(cur)
                cur += timedelta(days=1)

            print(f"Processing {len(dates)} dates...")
            total_outcomes = 0
            for i, d in enumerate(dates, 1):
                result = sync_results_for_date(engine, _to_iso(d))
                total_outcomes += result["outcomes"]
                if i < len(dates):
                    time.sleep(0.5)
            print(f"\nBackfill complete: {total_outcomes} outcomes across {len(dates)} dates")

        elif backfill_days > 0:
            yesterday = _default_target()
            dates = [yesterday - timedelta(days=i) for i in range(backfill_days + 1)]
            dates.reverse()
            print(f"Auto-backfill mode: yesterday + last {backfill_days} days ({len(dates)} dates total)")
            total_outcomes = 0
            for i, d in enumerate(dates, 1):
                result = sync_results_for_date(engine, _to_iso(d))
                total_outcomes += result["outcomes"]
                if i < len(dates):
                    time.sleep(0.5)
            print(f"\nAuto-backfill complete: {total_outcomes} outcomes across {len(dates)} dates")

        else:
            target = target_date or _to_iso(_default_target())
            print(f"Single-date mode: {target}")
            result = sync_results_for_date(engine, target)
            print(f"Result: {result}")

        log_row_counts(engine)

    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
