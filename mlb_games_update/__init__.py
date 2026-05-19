"""mlb_games_update — daily MLB schedule + probable pitchers sync.

Runs on GitHub Actions cron at 11:00 UTC (6am ET). Writes to the propgpt_mlb
schema in the shared Neon database. Self-contained — no imports from
sibling folders.

Entry point: `python -m mlb_games_update`
"""
