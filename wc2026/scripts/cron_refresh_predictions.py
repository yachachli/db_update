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
    get_projected_lineups_snapshot_summary,
    upsert_prediction,
)
from src.pipeline import bootstrap_tournament_pool, predict_matchup_by_id  # noqa: E402
from src.player_ratings import (  # noqa: E402
    snapshot_player_ratings_for_pool,
    snapshot_projected_lineups_for_pool,
)

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
    # Also env-overridable: how old a prediction must be before it is
    # re-predicted. A manual dispatch with 0 forces a rewrite of every
    # upcoming fixture in the window (e.g. after a model or data fix).
    max_age_hours = int(os.getenv("REFRESH_MAX_AGE_HOURS", "24"))
    # Re-fetch qualifier + WC-finals matches each run so the 5-match window
    # picks up newly played World Cup fixtures (league 732).
    force_pool_refresh = os.getenv("FORCE_POOL_REFRESH", "1").lower() not in (
        "0",
        "false",
        "no",
    )

    try:
        pool = bootstrap_tournament_pool(force_refresh=force_pool_refresh)
        fixtures = get_fixtures_needing_prediction(
            within_days=within_days, prediction_max_age_hours=max_age_hours
        )
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

    snap_stats: dict = {"rows_written": 0}
    lineup_stats: dict = {"rows_written": 0}

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

    print("\nPersisting projected_lineups_history snapshots (once per team)...")
    try:
        lineup_stats = snapshot_projected_lineups_for_pool(pool)
        print(
            f"  teams written: {lineup_stats['teams_written']}, "
            f"rows: {lineup_stats['rows_written']}, "
            f"xi_ok_teams: {lineup_stats['xi_ok_teams']}"
        )
        if lineup_stats.get("skipped_team_codes"):
            print(
                f"  skipped (no team_id): {lineup_stats['skipped_team_codes']}"
            )
        lineup_summary = get_projected_lineups_snapshot_summary()
        print(
            f"  snapshot_date: {lineup_summary['snapshot_date']}, "
            f"teams_with_xi: {lineup_summary['teams_with_xi']}, "
            f"xi_ok_rows: {lineup_summary['xi_ok_rows']}"
        )
    except DatabaseError as exc:
        logger.warning("Projected lineups snapshot persist failed (non-fatal): %s", exc)

    generated = 0
    skipped_unresolvable = 0
    skipped_missing_ids = 0
    skipped_upsert = 0
    errors = 0
    unresolvable_details: list[str] = []
    player_display_cache: dict[str, dict] = {}
    snapshot_rows = int(snap_stats.get("rows_written", 0))
    lineup_rows = int(lineup_stats.get("rows_written", 0))

    for fixture in fixtures:
        fixture_id = fixture["fixture_id"]
        team_a_id = fixture.get("team_a_id")
        team_b_id = fixture.get("team_b_id")
        team_a_name = fixture.get("team_a_name") or ""
        team_b_name = fixture.get("team_b_name") or ""

        if team_a_id is None or team_b_id is None:
            logger.warning(
                "Fixture %d (%s vs %s) missing team ids in Neon; skipping.",
                fixture_id, team_a_name, team_b_name,
            )
            skipped_missing_ids += 1
            unresolvable_details.append(
                f"fixture {fixture_id}: missing Neon team ids"
            )
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
            logger.warning(
                "Fixture %d (%s vs %s / ids %s, %s) skipped — pool team "
                "resolution failed: %s",
                fixture_id, team_a_name, team_b_name, team_a_id, team_b_id, exc,
            )
            skipped_unresolvable += 1
            unresolvable_details.append(
                f"fixture {fixture_id} ({team_a_name} vs {team_b_name}): {exc}"
            )
            continue
        except Exception as exc:
            logger.error(
                "Unexpected error predicting fixture %d (%s vs %s): %s",
                fixture_id, team_a_name, team_b_name, exc,
            )
            errors += 1
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
            skipped_upsert += 1
            errors += 1
            continue

        generated += 1
        logger.info("Predicted fixture %d: %s vs %s", fixture_id, team_a_name, team_b_name)

    attempted = len(fixtures)
    skipped_total = skipped_unresolvable + skipped_missing_ids + skipped_upsert
    if unresolvable_details:
        logger.warning(
            "Known pool-resolution skips (%d): %s",
            skipped_unresolvable,
            "; ".join(unresolvable_details),
        )

    if errors:
        exit_reason = f"{errors} genuine error(s) (upsert or unexpected)"
        exit_code = 1
    else:
        exit_reason = "completed (known skips logged as warnings)" if skipped_unresolvable else "completed"
        exit_code = 0

    print(
        f"\nRUN SUMMARY: attempted={attempted} predicted={generated} "
        f"skipped={skipped_total} (unresolvable={skipped_unresolvable}, "
        f"missing_ids={skipped_missing_ids}, upsert_fail={skipped_upsert}) "
        f"snapshot_rows={snapshot_rows} lineup_rows={lineup_rows} "
        f"exit={exit_code} ({exit_reason})"
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
