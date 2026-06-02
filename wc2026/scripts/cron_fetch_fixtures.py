"""Cron job: fetch WC 2026 finals fixtures from SportMonks into Neon.

Paginates through all fixtures from today through 2026-07-31 for league 732,
upserts each row into ``wc2026_fixtures``, and prints a summary.

Run from the project root:

    python scripts/cron_fetch_fixtures.py
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseError, get_connection, upsert_fixture, upsert_team  # noqa: E402
from src.models import Team  # noqa: E402
from src.sportmonks_client import (  # noqa: E402
    WC_FINALS_LEAGUE_ID,
    SportmonksClient,
    SportmonksError,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_END_DATE = date(2026, 7, 31)
_PER_PAGE = 100
_INCLUDE = "participants;state;venue;scores"

# SportMonks state developer_name / short_name -> our status field.
_COMPLETED_STATES = frozenset({
    "FT", "AET", "FT_PEN", "AWARDED", "WO", "COMPLETED", "FINISHED",
})
_IN_PROGRESS_STATES = frozenset({
    "INPLAY_1ST_HALF", "INPLAY_2ND_HALF", "HT", "BREAK", "ET", "PEN_LIVE",
    "INPLAY", "LIVE",
})


def _map_status(state: dict[str, Any] | None) -> str:
    if not state:
        return "scheduled"
    key = (
        state.get("developer_name")
        or state.get("short_name")
        or state.get("state")
        or ""
    ).upper()
    if key in _COMPLETED_STATES or key.startswith("FT"):
        return "completed"
    if key in _IN_PROGRESS_STATES or "INPLAY" in key or key == "LIVE":
        return "in_progress"
    return "scheduled"


def _participants(fixture: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (home, away) participant dicts."""
    home = away = None
    for p in fixture.get("participants") or []:
        meta = p.get("meta") or {}
        loc = meta.get("location")
        if loc == "home":
            home = p
        elif loc == "away":
            away = p
    participants = fixture.get("participants") or []
    if home is None and participants:
        home = participants[0]
    if away is None and len(participants) > 1:
        away = participants[1]
    if home is None or away is None:
        raise ValueError(f"Could not identify home/away for fixture {fixture.get('id')}")
    return home, away


def _parse_scheduled_at(fixture: dict[str, Any]) -> datetime:
    raw = fixture.get("starting_at")
    if isinstance(raw, str) and raw.strip():
        # SportMonks returns naive UTC timestamps like "2026-06-11 19:00:00".
        dt = datetime.fromisoformat(raw.replace(" ", "T"))
        return dt.replace(tzinfo=timezone.utc)
    ts = fixture.get("starting_at_timestamp")
    if ts:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    raise ValueError(f"Fixture {fixture.get('id')} has no scheduled time")


def _venue_city(fixture: dict[str, Any]) -> str | None:
    venue = fixture.get("venue")
    if not isinstance(venue, dict):
        return None
    return venue.get("city_name") or venue.get("name")


def _current_scores(
    fixture: dict[str, Any], home_id: int, away_id: int
) -> tuple[int | None, int | None, str | None]:
    """Extract full-time goals (home, away) and outcome from team_a (home) POV."""
    scores = fixture.get("scores") or []
    home_goals = away_goals = None
    for entry in scores:
        if entry.get("description") != "CURRENT":
            continue
        participant_id = entry.get("participant_id")
        goals = (entry.get("score") or {}).get("goals")
        if goals is None:
            continue
        if participant_id == home_id:
            home_goals = int(goals)
        elif participant_id == away_id:
            away_goals = int(goals)

    if home_goals is None or away_goals is None:
        return None, None, None

    if home_goals > away_goals:
        outcome = "W"
    elif home_goals < away_goals:
        outcome = "L"
    else:
        outcome = "D"
    return home_goals, away_goals, outcome


def _fetch_all_fixtures(client: SportmonksClient, start: date, end: date) -> list[dict]:
    """Paginate through the fixtures/between endpoint until ``has_more`` is false."""
    fixtures: list[dict] = []
    page = 1
    while True:
        response = client.get(
            f"fixtures/between/{start.isoformat()}/{end.isoformat()}",
            params={
                "filters": f"fixtureLeagues:{WC_FINALS_LEAGUE_ID}",
                "include": _INCLUDE,
                "per_page": _PER_PAGE,
                "page": page,
            },
        )
        batch = response.get("data")
        if isinstance(batch, list):
            fixtures.extend(batch)
        pagination = response.get("pagination") or {}
        logger.info(
            "Fetched page %s (%d fixtures this page, %d total)",
            pagination.get("current_page", page),
            len(batch) if isinstance(batch, list) else 0,
            len(fixtures),
        )
        if not pagination.get("has_more"):
            break
        page += 1
    return fixtures


def _fixture_exists(sportmonks_fixture_id: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM wc2026_fixtures WHERE sportmonks_fixture_id = %s",
            (sportmonks_fixture_id,),
        ).fetchone()
    return row is not None


def _ensure_teams(home: dict[str, Any], away: dict[str, Any]) -> None:
    """Upsert minimal team rows so fixture FK constraints succeed."""
    for participant in (home, away):
        team = Team(
            team_id=int(participant["id"]),
            name=str(participant.get("name", "")),
            confederation="UNKNOWN",
            fifa_points=0.0,
            fifa_rank=0,
            is_host=False,
        )
        upsert_team(team, fifa_code=participant.get("short_code"))


def _fixture_to_row(fixture: dict[str, Any]) -> dict[str, Any]:
    home, away = _participants(fixture)
    _ensure_teams(home, away)
    status = _map_status(fixture.get("state"))
    home_id, away_id = int(home["id"]), int(away["id"])

    row: dict[str, Any] = {
        "sportmonks_fixture_id": int(fixture["id"]),
        "team_a_id": home_id,
        "team_b_id": away_id,
        "team_a_name": home.get("name"),
        "team_b_name": away.get("name"),
        "scheduled_at": _parse_scheduled_at(fixture),
        "venue_city": _venue_city(fixture),
        "round": None,  # v1: SportMonks group/stage metadata not mapped yet
        "status": status,
    }

    if status == "completed":
        home_goals, away_goals, outcome = _current_scores(fixture, home_id, away_id)
        row["actual_home_goals"] = home_goals
        row["actual_away_goals"] = away_goals
        row["actual_outcome"] = outcome

    return row


def main() -> int:
    start = date.today()
    print("=" * 78)
    print("CRON: FETCH WC 2026 FIXTURES")
    print("=" * 78)
    print(f"Date range: {start.isoformat()} -> {_END_DATE.isoformat()}  league={WC_FINALS_LEAGUE_ID}")

    try:
        client = SportmonksClient()
        fixtures = _fetch_all_fixtures(client, start, _END_DATE)
    except (SportmonksError, DatabaseError) as exc:
        print(f"ERROR: fetch failed: {exc}")
        return 1

    new_count = 0
    updated_count = 0

    try:
        for fixture in fixtures:
            sm_id = int(fixture["id"])
            existed = _fixture_exists(sm_id)
            upsert_fixture(_fixture_to_row(fixture))
            if existed:
                updated_count += 1
            else:
                new_count += 1
    except (DatabaseError, ValueError) as exc:
        print(f"ERROR: upsert failed: {exc}")
        return 1

    print(f"\nFetched {len(fixtures)} fixtures, {new_count} new, {updated_count} updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
