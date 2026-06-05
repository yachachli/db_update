"""Cron job: refresh predictions for upcoming WC 2026 fixtures in Neon.

Bootstraps the tournament pool, finds fixtures needing a fresh prediction,
runs ``predict_matchup`` for each, and upserts the result.

Run from the project root:

    python scripts/cron_refresh_predictions.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import (  # noqa: E402
    DatabaseError,
    get_fixtures_needing_prediction,
    get_player_ratings_snapshot_summary,
    upsert_prediction,
)
from src.pipeline import bootstrap_tournament_pool, predict_matchup_by_id  # noqa: E402
from src.player_ratings import snapshot_player_ratings_for_pool  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    print("=" * 78)
    print("CRON: REFRESH PREDICTIONS")
    print("=" * 78)

    # Window is env-overridable so a manual workflow_dispatch can backfill
    # fixtures further out than the daily 3-day horizon (e.g. an opening-slate
    # populate before the tournament starts). Scheduled runs leave it at 3.
    within_days = int(os.getenv("REFRESH_WITHIN_DAYS", "3"))

    try:
        pool = bootstrap_tournament_pool()
        fixtures = get_fixtures_needing_prediction(within_days=within_days)
    except DatabaseError as exc:
        print(f"ERROR: setup failed: {exc}")
        return 1
    except Exception as exc:
        print(f"ERROR: pool bootstrap failed: {exc}")
        return 1

    print(
        f"Pool: {len(pool.teams)} teams. Window: {within_days}d. "
        f"Fixtures needing prediction: {len(fixtures)}"
    )

    print("\nPersisting player_ratings_history snapshots (once per team)...")
    try:
        snap_stats = snapshot_player_ratings_for_pool(pool)
        print(
            f"  teams written: {snap_stats['teams_written']}, "
            f"rows: {snap_stats['rows_written']}, "
            f"by_source: {snap_stats['by_source']}"
        )
        if snap_stats.get("skipped_team_codes"):
            print(
                f"  skipped (no team_id): {snap_stats['skipped_team_codes']}"
            )
        summary = get_player_ratings_snapshot_summary()
        print(f"  snapshot_date: {summary['snapshot_date']}")
        distinct_teams = len({r["team_code"] for r in summary["by_team_source"]})
        print(f"  distinct teams in history today: {distinct_teams}")
    except DatabaseError as exc:
        logger.warning("Player ratings snapshot persist failed (non-fatal): %s", exc)

    generated = 0
    skipped = 0
    unresolvable = 0
    player_display_cache: dict[str, dict] = {}

    for fixture in fixtures:
        fixture_id = fixture["fixture_id"]
        team_a_id = fixture.get("team_a_id")
        team_b_id = fixture.get("team_b_id")
        team_a_name = fixture.get("team_a_name") or ""
        team_b_name = fixture.get("team_b_name") or ""

        if team_a_id is None or team_b_id is None:
            logger.error(
                "Fixture %d missing team ids; skipping.", fixture_id,
            )
            skipped += 1
            continue

        # Resolve by SportMonks id, not name: the fixtures table's names
        # ("Czech Republic", "Cape Verde Islands", ...) don't match the pool's
        # SportMonks names, which silently skipped fixtures under name lookup.
        try:
            report_json = predict_matchup_by_id(
                int(team_a_id),
                int(team_b_id),
                pool,
                player_display_cache=player_display_cache,
            )
        except ValueError as exc:
            logger.error(
                "Could not resolve teams for fixture %d (%s vs %s / ids %s, %s): %s",
                fixture_id, team_a_name, team_b_name, team_a_id, team_b_id, exc,
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
