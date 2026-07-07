"""mlb_odds_update — intraday sportsbook odds snapshots for the MLB slate.

Runs several times a day on GitHub Actions. Snapshots full-game and first-5-innings
(F5) odds from The Odds API into the propgpt_mlb.odds_snapshots table of the shared
Neon database. Self-contained — no imports from sibling folders.

Entry point: `python -m mlb_odds_update`
"""
