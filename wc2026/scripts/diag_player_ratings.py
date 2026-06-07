"""Phase 0 diagnostic: verify SportMonks returns per-player RATING (type_id 118)
in national-team fixture lineups.

Uses the existing SportmonksClient only (no src/ changes). Resolves Estonia,
fetches the 3 most recent fixtures with lineup detail, prints rating rows, and
exits non-zero if zero ratings are found.

Run from the project root:

    python scripts/diag_player_ratings.py
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_client import (  # noqa: E402
    QUALIFIER_LEAGUE_IDS,
    SportmonksClient,
    SportmonksError,
)

logger = logging.getLogger("diag_player_ratings")

SEARCH_TEAM = "Estonia"
FIXTURE_LIMIT = 3
RATING_TYPE_ID = 118

# Mirror SportmonksClient._FIXTURE_INCLUDE; append lineup detail (do not drop existing).
_BASE_FIXTURE_INCLUDE = "xGFixture;participants;scores;state;venue"
_FIXTURE_INCLUDE = f"{_BASE_FIXTURE_INCLUDE};lineups.details.type"

_FIXTURE_LOOKBACK_DAYS = 365 * 4
_FIXTURE_FETCH_PAGE = 100


def resolve_team_id(client: SportmonksClient) -> tuple[int, str]:
    """Search for the senior men's national team (same approach as test_fetcher_live)."""
    response = client.get(f"teams/search/{SEARCH_TEAM}")
    candidates = response.get("data")
    if not isinstance(candidates, list) or not candidates:
        raise SportmonksError(f"No teams found for search '{SEARCH_TEAM}'.")

    def is_national(team: dict[str, Any]) -> bool:
        return team.get("type") == "national"

    exact = [
        t
        for t in candidates
        if is_national(t) and str(t.get("name", "")).lower() == SEARCH_TEAM.lower()
    ]
    nationals = [t for t in candidates if is_national(t)]
    chosen = (exact or nationals or candidates)[0]
    return int(chosen["id"]), str(chosen.get("name", ""))


def fetch_recent_fixtures_with_lineups(
    client: SportmonksClient, team_id: int, limit: int
) -> list[dict[str, Any]]:
    """Same date-ranged endpoint as get_fixtures_for_team, with lineup includes."""
    end = date.today()
    start = end - timedelta(days=_FIXTURE_LOOKBACK_DAYS)
    league_csv = ",".join(str(lid) for lid in QUALIFIER_LEAGUE_IDS)
    filter_parts = [f"fixtureLeagues:{league_csv}"]

    response = client.get(
        f"fixtures/between/{start.isoformat()}/{end.isoformat()}/{team_id}",
        params={
            "filters": ";".join(filter_parts),
            "include": _FIXTURE_INCLUDE,
            "per_page": _FIXTURE_FETCH_PAGE,
        },
    )
    data = response.get("data")
    if not isinstance(data, list):
        return []

    data.sort(key=lambda f: f.get("starting_at_timestamp") or 0, reverse=True)
    return data[:limit]


def _player_name(lineup_row: dict[str, Any]) -> str:
    name = lineup_row.get("player_name") or lineup_row.get("name")
    if name:
        return str(name)
    player = lineup_row.get("player")
    if isinstance(player, dict):
        return str(player.get("display_name") or player.get("name") or "?")
    return "?"


def _detail_value(details: list[Any], type_id: int) -> Any | None:
    for detail in details:
        if not isinstance(detail, dict):
            continue
        if detail.get("type_id") == type_id:
            return detail.get("data", {}).get("value") if isinstance(
                detail.get("data"), dict
            ) else detail.get("value")
    return None


def extract_team_lineup_ratings(
    fixture: dict[str, Any], team_id: int
) -> list[tuple[str, str | float]]:
    """Return (player_name, rating_or_MISSING) for our team's lineup players."""
    lineups = fixture.get("lineups")
    if not isinstance(lineups, list):
        return []

    rows: list[tuple[str, str | float]] = []
    for row in lineups:
        if not isinstance(row, dict):
            continue
        row_team = row.get("team_id") or row.get("participant_id")
        if row_team is not None and int(row_team) != int(team_id):
            continue

        details = row.get("details")
        if not isinstance(details, list):
            details = []

        rating = _detail_value(details, RATING_TYPE_ID)
        display = rating if rating is not None else "MISSING"
        rows.append((_player_name(row), display))
    return rows


def _fixture_date(fixture: dict[str, Any]) -> str:
    return str(fixture.get("starting_at") or fixture.get("date") or "?")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )

    print("=" * 70)
    print("Phase 0: player RATING (type_id 118) diagnostic")
    print("=" * 70)

    try:
        client = SportmonksClient()
    except SportmonksError as exc:
        print(f"\n[CONFIG ERROR] {exc}")
        return 1

    try:
        team_id, team_name = resolve_team_id(client)
        print(f"Team: {team_name!r} (id={team_id})")

        fixtures = fetch_recent_fixtures_with_lineups(
            client, team_id, FIXTURE_LIMIT
        )
        print(f"Fixtures fetched (limit={FIXTURE_LIMIT}): {len(fixtures)}")

        if not fixtures:
            print("\n[ERROR] No fixtures returned — cannot verify ratings.")
            return 1

        present = 0
        missing = 0

        print("\nPer-player rating rows:")
        print("-" * 70)
        for fixture in fixtures:
            fid = fixture.get("id")
            fdate = _fixture_date(fixture)
            players = extract_team_lineup_ratings(fixture, team_id)
            if not players:
                print(f"  fixture_id={fid} date={fdate} — no lineup rows for team_id={team_id}")
                continue
            for player_name, rating in players:
                label = "present" if rating != "MISSING" else "MISSING"
                if rating != "MISSING":
                    present += 1
                else:
                    missing += 1
                print(
                    f"  fixture_id={fid}  date={fdate}  "
                    f"player={player_name!r}  rating={rating}  ({label})"
                )

        total = present + missing
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Player-fixture rows total : {total}")
        print(f"  With real rating        : {present}")
        print(f"  MISSING                 : {missing}")

        if present == 0:
            print(
                "\n[FAIL] Zero ratings across all fixtures — "
                "include plan may not return RATING data. Stopping."
            )
            return 1

        print("\n[OK] At least one RATING value returned — Phase 0 passed.")
        return 0

    except SportmonksError as exc:
        print(f"\n[REQUEST FAILED] {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"\n[UNEXPECTED ERROR] {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
