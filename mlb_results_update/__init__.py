"""mlb_results_update — nightly MLB outcomes + box score ingestion.

Runs on GitHub Actions cron at 09:00 UTC (5am ET). Writes yesterday's
game results to the propgpt_mlb schema in the shared Neon database.

Entry point: `python -m mlb_results_update`

Tables populated:
  - outcomes (final scores)
  - pitcher_game_logs (per-pitcher line — starter + relievers)
  - team_game_logs (per-team batting summary)
  - players (enriched with handedness/position when missing)
"""
