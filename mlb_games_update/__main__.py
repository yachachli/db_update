"""Entry point: python -m mlb_games_update

Modes (chosen by env vars):
  - Manual date range: BACKFILL_START=YYYYMMDD + BACKFILL_END=YYYYMMDD
  - Auto rolling backfill: BACKFILL_DAYS=N (re-runs the last N days)
  - Single date: TARGET_DATE=YYYY-MM-DD
  - Default: today + tomorrow (US/Eastern)

Default dual-date sync catches late probable-pitcher announcements on today's
slate and early listings for tomorrow's games. Upserts are idempotent —
COALESCE keeps an existing SP id when the API still returns null.
"""
import os
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .pipeline import (
    bootstrap_teams_and_parks_if_empty,
    get_engine,
    sync_games_for_date,
    log_row_counts,
)

ET_ZONE = ZoneInfo("America/New_York")


def _to_iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _today_et() -> date:
    return datetime.now(ET_ZONE).date()


def _sync_dates(engine, dates: list[date]) -> dict:
    total_games = 0
    failed_dates: list[str] = []
    for i, d in enumerate(dates, 1):
        iso = _to_iso(d)
        try:
            result = sync_games_for_date(engine, iso)
            total_games += result["games"]
            print(f"  {iso}: {result}")
        except Exception as e:
            print(f"  DATE FAILED {iso}: {e}", file=sys.stderr)
            failed_dates.append(iso)
        if i < len(dates):
            time.sleep(0.5)
    return {"games": total_games, "dates": len(dates), "failed_dates": failed_dates}


def main() -> None:
    try:
        engine = get_engine()

        backfill_start = os.getenv("BACKFILL_START")  # YYYYMMDD
        backfill_end = os.getenv("BACKFILL_END")      # YYYYMMDD
        backfill_days = int(os.getenv("BACKFILL_DAYS") or "0")
        target_date = (os.getenv("TARGET_DATE") or "").strip()  # YYYY-MM-DD

        is_backfill = bool((backfill_start and backfill_end) or backfill_days > 0)
        if is_backfill:
            # Per-request politeness pacing, active only for long historical runs.
            os.environ.setdefault("MLB_REQUEST_SLEEP_SEC", "0.4")

        # Bootstrap teams+parks once (idempotent — no-op if 30 teams already present)
        bootstrap_teams_and_parks_if_empty(engine, season=_today_et().year)

        if backfill_start and backfill_end:
            start = _parse_yyyymmdd(backfill_start)
            end = _parse_yyyymmdd(backfill_end)
            print(f"Manual backfill mode: {start} to {end}")
            dates: list[date] = []
            cur = start
            while cur <= end:
                dates.append(cur)
                cur += timedelta(days=1)

            print(f"Processing {len(dates)} dates...")
            result = _sync_dates(engine, dates)
            print(
                f"\nBackfill complete: {result['games']} games upserted "
                f"across {result['dates']} dates"
            )
            if result["failed_dates"]:
                print(f"{len(result['failed_dates'])} date(s) FAILED: {result['failed_dates']}")

        elif backfill_days > 0:
            today = _today_et()
            dates = [today - timedelta(days=i) for i in range(backfill_days + 1)]
            dates.reverse()
            print(
                f"Auto-backfill mode (ET): last {backfill_days} days + today "
                f"({len(dates)} dates total)"
            )
            result = _sync_dates(engine, dates)
            print(
                f"\nAuto-backfill complete: {result['games']} games upserted "
                f"across {result['dates']} dates"
            )
            if result["failed_dates"]:
                print(f"{len(result['failed_dates'])} date(s) FAILED: {result['failed_dates']}")

        elif target_date:
            print(f"Single-date mode: {target_date}")
            result = sync_games_for_date(engine, target_date)
            print(f"Result: {result}")

        else:
            # Live default: today + tomorrow ET so afternoon/evening cron passes
            # pick up late probable announcements and next-day early listings.
            today = _today_et()
            dates = [today, today + timedelta(days=1)]
            print(
                f"Live probable-refresh mode (ET): "
                f"{_to_iso(dates[0])} + {_to_iso(dates[1])}"
            )
            result = _sync_dates(engine, dates)
            print(
                f"\nLive sync complete: {result['games']} games upserted "
                f"across {result['dates']} dates"
            )
            if result["failed_dates"]:
                print(f"{len(result['failed_dates'])} date(s) FAILED: {result['failed_dates']}")
                raise SystemExit(1)

        log_row_counts(engine)

    except SystemExit:
        raise
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
