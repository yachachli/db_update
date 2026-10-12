"""Timezone-aware calendar-date helpers shared by NBA update jobs."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PACIFIC_TIMEZONE = ZoneInfo("America/Los_Angeles")
EASTERN_TIMEZONE = ZoneInfo("America/New_York")


def daily_fetch_dates(now: datetime | None = None) -> tuple[datetime, str, str]:
    """Return Pacific now plus the prior/current calendar dates for the daily run."""
    if now is None:
        now_pacific = datetime.now(PACIFIC_TIMEZONE)
    elif now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    else:
        now_pacific = now.astimezone(PACIFIC_TIMEZONE)

    yesterday = (now_pacific - timedelta(days=1)).strftime("%Y%m%d")
    today = now_pacific.strftime("%Y%m%d")
    return now_pacific, yesterday, today


def game_date_from_commence(commence: str, now: datetime | None = None) -> str:
    """Convert an Odds API UTC start time to the NBA's Eastern slate date."""
    try:
        dt = datetime.fromisoformat(commence.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(EASTERN_TIMEZONE).strftime("%Y-%m-%d")
    except (AttributeError, TypeError, ValueError):
        if now is None:
            now = datetime.now(EASTERN_TIMEZONE)
        elif now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return now.astimezone(EASTERN_TIMEZONE).strftime("%Y-%m-%d")
