# BracketIQ — GitHub Export & Neon Setup

Two-repo pattern for deployment with Neon PostgreSQL.

## Repo 1: bracketiq-db-update (nightly runner)

**Purpose:** Runs on schedule (e.g. 8 AM UTC) to refresh data and push to Neon.

**Contents:**
- `.github/workflows/nightly_update.yml` — Cron + manual trigger
- `scripts/`: refresh_kenpom_cache, collect_historical_fanmatch, collect_historical_odds, build_ats_dataset, slate_today, **push_to_neon**
- `app/`: config, scrapers (kenpom, odds), services (team_name_resolver)
- `data/`: team_name_mapping.json, kenpom_aliases.json
- `requirements.txt`, `.env.example`

**Secrets (GitHub repo secrets):**
- `KENPOM_EMAIL`, `KENPOM_PASSWORD`
- `ODDS_API_KEY`
- `NEON_DATABASE_URL`

**Flow:** Checkout → install deps → refresh KenPom → incremental FanMatch → incremental odds → build ATS → slate_today → push_to_neon. Parquets are used only during the run; Neon is the persistent store.

## Repo 2: bracketiq (main project / API)

**Purpose:** FastAPI app that reads from Neon and serves slate, teams, matchups.

**Difference from current:** Services read from Neon tables instead of parquet files:
- `slate_service`: `SELECT * FROM slate_today`
- `team_service`: `SELECT * FROM kenpom_ratings` (or team_profiles table)
- `ats_service`: `SELECT * FROM ats_historical`

**Still needed:** `data/team_name_mapping.json`, `kenpom_aliases.json` for any live resolution in the API.

## Neon schema

Run `data/neon_schema.sql` once in the Neon SQL editor to create tables. Thereafter `push_to_neon.py` uses `if_exists='replace'` for full refresh of each table.

## Development order

1. Fix name resolution and model adjustments (done in main codebase).
2. Add push_to_neon.py and run locally with NEON_DATABASE_URL.
3. Create Neon project, run schema, test push.
4. Create bracketiq-db-update repo, add workflow and secrets, run manually.
5. Refactor main project to read from Neon; deploy.
