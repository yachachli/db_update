# WC 2026 Predictor — db_update integration

Daily/weekly cron jobs that keep Neon tables fresh for the World Cup 2026
predictive model. Lives in **yachachli/db_update** alongside MLB/NBA/BracketIQ
update folders.

## Workflows (`.github/workflows/`)

| Workflow | Schedule | Script |
| --- | --- | --- |
| `wc2026_fetch_fixtures.yml` | Daily 06:00 UTC | `scripts/cron_fetch_fixtures.py` |
| `wc2026_fetch_fifa.yml` | Mondays 08:00 UTC | `scripts/cron_fetch_fifa.py` |
| `wc2026_refresh_predictions.yml` | Daily 10:00 UTC | `scripts/cron_refresh_predictions.py` |

## GitHub Secrets (db_update repo)

| Secret | Used by |
| --- | --- |
| `DB_URL` | All three jobs (mapped to `NEON_DATABASE_URL` in workflow YAML) |
| `SPORTMONKS_API_KEY` | Fixture fetch + prediction refresh |

## Local setup

```bash
cd wc2026
pip install -r requirements.txt
cp .env.example .env   # fill in DB_URL and SPORTMONKS_API_KEY

python scripts/setup_neon_schema.py      # once
python scripts/populate_neon_initial.py  # once (teams + ratings + FIFA)
python scripts/cron_fetch_fixtures.py
python scripts/cron_fetch_fifa.py
python scripts/cron_refresh_predictions.py
python scripts/test_neon_connection.py
```

## Neon tables

`teams`, `team_ratings`, `fifa_rankings`, `wc2026_fixtures`, `predictions`,
`wc2026_squads`, `player_id_map`, `team_player_ratings`, `player_ratings_history`

Source of truth for model code: [zernerdoescode/wc2026-predictor](https://github.com/zernerdoescode/wc2026-predictor).
Copy `src/` + cron scripts here when updating.
