"""Cron job: refresh predictions for upcoming WC 2026 fixtures in Neon.

Bootstraps the tournament pool, finds fixtures needing a fresh prediction,
runs ``predict_matchup`` for each, and upserts the result.

Run from the project root:

    python scripts/cron_refresh_predictions.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import (  # noqa: E402
    DatabaseError,
    get_fixtures_needing_prediction,
    upsert_prediction,
)
from src.pipeline import bootstrap_tournament_pool, predict_matchup  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    print("=" * 78)
    print("CRON: REFRESH PREDICTIONS")
    print("=" * 78)

    try:
        pool = bootstrap_tournament_pool()
        fixtures = get_fixtures_needing_prediction(within_days=3)
    except DatabaseError as exc:
        print(f"ERROR: setup failed: {exc}")
        return 1
    except Exception as exc:
        print(f"ERROR: pool bootstrap failed: {exc}")
        return 1

    print(f"Pool: {len(pool.teams)} teams. Fixtures needing prediction: {len(fixtures)}")

    generated = 0
    skipped = 0
    unresolvable = 0

    for fixture in fixtures:
        fixture_id = fixture["fixture_id"]
        team_a_name = fixture.get("team_a_name") or ""
        team_b_name = fixture.get("team_b_name") or ""

        if not team_a_name or not team_b_name:
            logger.error(
                "Fixture %d missing team names; skipping.", fixture_id,
            )
            skipped += 1
            continue

        try:
            report_json = predict_matchup(team_a_name, team_b_name, pool)
        except ValueError as exc:
            logger.error(
                "Could not resolve teams for fixture %d (%s vs %s): %s",
                fixture_id, team_a_name, team_b_name, exc,
            )
            unresolvable += 1
            skipped += 1
            continue

        report_dict = json.loads(report_json)
        try:
            upsert_prediction(
                fixture_id,
                report_dict,
                team_a_id=int(fixture["team_a_id"]),
                team_b_id=int(fixture["team_b_id"]),
            )
        except DatabaseError as exc:
            logger.error("Failed to upsert prediction for fixture %d: %s", fixture_id, exc)
            skipped += 1
            continue

        generated += 1
        logger.info("Predicted fixture %d: %s vs %s", fixture_id, team_a_name, team_b_name)

    print(
        f"\nGenerated {generated} predictions, {skipped} skipped "
        f"({unresolvable} unresolvable teams)."
    )
    if fixtures and generated == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
