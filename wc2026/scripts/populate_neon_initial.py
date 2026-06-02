"""One-time population of Neon from the local tournament pool.

Upserts teams, current ratings, and the latest FIFA ranking release.
Fixtures and predictions are populated by cron jobs, not here.

Run from the project root:

    python scripts/populate_neon_initial.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.aggregation import compute_team_rating  # noqa: E402
from src.database import (  # noqa: E402
    DatabaseError,
    upsert_fifa_ranking,
    upsert_team,
    upsert_team_rating,
)
from src.pipeline import bootstrap_tournament_pool  # noqa: E402

_DATA_SOURCE_QUALIFIER = "qualifier_matches"
_DATA_SOURCE_HOST = "synthetic_host_override"


def _fifa_code_for_team(pool, team) -> str | None:
    """Best-effort FIFA country code from the pool's ranking release."""
    entry = pool.fifa_release.lookup_by_name(team.name)
    return entry.country_code if entry else None


def main() -> int:
    print("=" * 78)
    print("NEON INITIAL POPULATION")
    print("=" * 78)

    print("\nLoading tournament pool (cached if fresh)...")
    try:
        pool = bootstrap_tournament_pool()
    except Exception as exc:
        print(f"ERROR: could not load tournament pool: {exc}")
        return 1

    roster_ids = sorted(set(pool.matches_by_team) | set(pool.host_ratings))
    print(f"Pool: {len(pool.teams)} teams in cache ({len(roster_ids)} roster), "
          f"FIFA release {pool.fifa_release.release_date}")

    team_count = 0
    rating_count = 0
    fifa_count = 0

    try:
        for team_id in roster_ids:
            team = pool.teams[team_id]
            fifa_code = _fifa_code_for_team(pool, team)
            upsert_team(team, fifa_code=fifa_code)
            team_count += 1

            if team_id in pool.host_ratings:
                rating = pool.host_ratings[team_id]
                data_source = _DATA_SOURCE_HOST
            else:
                matches = pool.matches_by_team[team_id]
                rating = compute_team_rating(team, matches, pool.baseline)
                data_source = _DATA_SOURCE_QUALIFIER

            upsert_team_rating(rating, data_source)
            rating_count += 1

        release_date = date.fromisoformat(pool.fifa_release.release_date)
        for entry in pool.fifa_release.entries:
            upsert_fifa_ranking(entry, release_date)
            fifa_count += 1

    except DatabaseError as exc:
        print(f"\nERROR: database operation failed: {exc}")
        return 1
    except Exception as exc:
        print(f"\nERROR: population failed: {exc}")
        return 1

    print(f"\nPopulated {team_count} teams, {rating_count} ratings, "
          f"{fifa_count} FIFA rankings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
