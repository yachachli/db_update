# mlb_results_update

Nightly MLB outcomes + box score ingestion. Part of the PropGPT MLB prediction model pipeline.

## What it does

At 09:00 UTC (5am ET) each day, this folder's GitHub Actions workflow runs `python -m mlb_results_update`, which:

1. Connects to the shared Neon database (via the `DATABASE_URL` secret)
2. Fetches yesterday's MLB schedule from the free public MLB Stats API
3. For each game that is Final:
   - Writes the outcome (scores, total runs, extra innings flag) to `outcomes`
   - Writes per-pitcher lines (IP/ER/K/BB/HR/etc) to `pitcher_game_logs` for every pitcher who appeared
   - Writes per-team batting summaries to `team_game_logs`, denormalizing opposing starter handedness
4. Enriches any pitcher who appeared but didn't yet have `throws`/`bats`/etc populated, by calling `/people/{id}`

All writes are idempotent (ON CONFLICT DO UPDATE), so re-running is safe and overwrites with the latest box score data (e.g. official scorer corrections).

## Tables touched

All in the `propgpt_mlb` schema:
- `outcomes`
- `pitcher_game_logs`
- `team_game_logs`
- `players` (enrichment of stubs created by `mlb_games_update`)

Tables NOT touched by this folder:
- `games` (owned by `mlb_games_update`)
- `bullpen_appearances` (deferred — needs leverage index, which boxscores don't expose)
- `odds_snapshots`, `weather_observations`, etc. (separate future folders)

## Operation modes

Selected by env var (first match wins):

| Mode | Env vars | Use case |
|---|---|---|
| Manual range | `BACKFILL_START=YYYYMMDD` + `BACKFILL_END=YYYYMMDD` | Historical season ingest, gap fills |
| Auto rolling | `BACKFILL_DAYS=N` | Catch corrections to recent games |
| Single date | `TARGET_DATE=YYYY-MM-DD` | Test a specific date |
| Default | (none) | Yesterday (UTC date - 1) |

## Schedule

`.github/workflows/mlb_results_update.yml` runs at `0 9 * * *` UTC (5am ET).

5am ET is late enough that even extra-inning West Coast games (which can end past 1am ET) are complete, and early enough that downstream pipelines have a full picture by the time `mlb_games_update` runs at 6am ET for the new day.

Also supports `workflow_dispatch` from the GitHub UI.

## Required secrets

- `DATABASE_URL` — shared Neon connection string

No API key required (MLB Stats API is free + public).

## Running manually (local)

```bash
cd mlb_results_update
pip install -r requirements.txt
cp .env.example .env  # edit and add your DATABASE_URL

# Yesterday (default)
DATABASE_URL="postgresql://..." python -m mlb_results_update

# Specific date
DATABASE_URL="postgresql://..." TARGET_DATE="2026-05-14" python -m mlb_results_update

# Rolling 7-day backfill
DATABASE_URL="postgresql://..." BACKFILL_DAYS=7 python -m mlb_results_update

# Explicit historical range
DATABASE_URL="postgresql://..." BACKFILL_START=20260401 BACKFILL_END=20260430 \
    python -m mlb_results_update
```

## Failure handling

- Exits 0 on success, 1 on failure
- Per-game failures are logged and skipped (don't abort the run)
- All writes are idempotent

## Schema sync

The writer SQL parallels other `db_update` folders (`mlb_games_update`). When the `propgpt_mlb` schema changes, update all sibling folders. All SQL is schema-qualified (`propgpt_mlb.X`) for Neon pooled-endpoint compatibility.
