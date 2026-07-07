"""Entry point: python -m mlb_odds_update

Modes:
  - Single date:  TARGET_DATE=YYYY-MM-DD  (interpreted as a US/Eastern slate date)
  - Default:      today (US/Eastern)

No historical backfill here — The Odds API historical endpoint is a separate step.
"""
import os
import sys
from datetime import date, datetime

from .pipeline import (
    default_target_date,
    get_engine,
    log_row_counts,
    sync_odds_for_date,
)


def _parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    try:
        engine = get_engine()

        target_date_env = os.getenv("TARGET_DATE")
        target = _parse_iso_date(target_date_env) if target_date_env else default_target_date()

        print(f"Single-date mode (ET): {target}")
        result = sync_odds_for_date(engine, target)
        print(f"Result: {result}")

        log_row_counts(engine)

    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
