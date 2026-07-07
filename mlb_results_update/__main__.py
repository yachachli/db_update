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

        is_backfill = bool((backfill_start and backfill_end) or backfill_days > 0)
        if is_backfill:
            # Per-request politeness pacing, active only for long historical runs.
            os.environ.setdefault("MLB_REQUEST_SLEEP_SEC", "0.4")

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
            failed_dates: list[str] = []
            for i, d in enumerate(dates, 1):
                iso = _to_iso(d)
                try:
                    result = sync_results_for_date(engine, iso)
                    total_outcomes += result["outcomes"]
                except Exception as e:
                    # A failed date logs and continues so a multi-week run never aborts.
                    print(f"  DATE FAILED {iso}: {e}", file=sys.stderr)
                    failed_dates.append(iso)
                if i < len(dates):
                    time.sleep(0.5)
            print(f"\nBackfill complete: {total_outcomes} outcomes across {len(dates)} dates")
            if failed_dates:
                print(f"{len(failed_dates)} date(s) FAILED: {failed_dates}")

        elif backfill_days > 0:
            yesterday = _default_target()
            dates = [yesterday - timedelta(days=i) for i in range(backfill_days + 1)]
            dates.reverse()
            print(f"Auto-backfill mode: yesterday + last {backfill_days} days ({len(dates)} dates total)")
            total_outcomes = 0
            failed_dates = []
            for i, d in enumerate(dates, 1):
                iso = _to_iso(d)
                try:
                    result = sync_results_for_date(engine, iso)
                    total_outcomes += result["outcomes"]
                except Exception as e:
                    print(f"  DATE FAILED {iso}: {e}", file=sys.stderr)
                    failed_dates.append(iso)
                if i < len(dates):
                    time.sleep(0.5)
            print(f"\nAuto-backfill complete: {total_outcomes} outcomes across {len(dates)} dates")
            if failed_dates:
                print(f"{len(failed_dates)} date(s) FAILED: {failed_dates}")

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
