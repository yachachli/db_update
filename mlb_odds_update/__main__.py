"""Entry point: python -m mlb_odds_update

Modes:
  - Single date (live):     TARGET_DATE=YYYY-MM-DD  (US/Eastern slate date)
  - Default (live):         today (US/Eastern)
  - Historical backfill:    BACKFILL_START=YYYYMMDD  BACKFILL_END=YYYYMMDD
                            Optional: INCLUDE_F5=true, ODDS_REQUEST_SLEEP_SEC=1.0
"""
import os
import sys
from datetime import date, datetime, timedelta

from .pipeline import (
    default_target_date,
    get_engine,
    log_row_counts,
    sync_historical_odds_range,
    sync_odds_for_date,
)


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "y"}


def main() -> None:
    try:
        engine = get_engine()

        backfill_start = os.getenv("BACKFILL_START")
        backfill_end = os.getenv("BACKFILL_END")
        if backfill_start and backfill_end:
            start = _parse_yyyymmdd(backfill_start)
            end = _parse_yyyymmdd(backfill_end)
            include_f5 = _truthy(os.getenv("INCLUDE_F5"))
            print(f"Historical backfill mode: {start} -> {end}  include_f5={include_f5}")
            result = sync_historical_odds_range(
                engine,
                start,
                end,
                include_f5=include_f5,
                skip_existing=True,
            )
            print(f"Result: {result}")
            log_row_counts(engine)
            if result.get("failed_dates"):
                print(f"FAILED DATES: {result['failed_dates']}", file=sys.stderr)
                raise SystemExit(1)
            return

        target_date_env = os.getenv("TARGET_DATE")
        target = _parse_iso_date(target_date_env) if target_date_env else default_target_date()

        print(f"Single-date live mode (ET): {target}")
        result = sync_odds_for_date(engine, target)
        print(f"Result: {result}")

        log_row_counts(engine)

    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
