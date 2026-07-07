# mlb_games_update

Daily MLB schedule + probable pitchers sync. Part of the PropGPT MLB prediction model pipeline.

## What it does

At 11:00 UTC (6am ET) each day, this folder's GitHub Actions workflow runs `python -m mlb_games_update`, which:

1. Connects to the shared Neon database (via the `DATABASE_URL` secret)
2. On first run only: populates the 30 MLB teams and their home parks
3. Fetches today's MLB slate from the free public MLB Stats API (`statsapi.mlb.com`)
4. Upserts each game (with probable pitchers when announced) into `propgpt_mlb.games`

The script is **idempotent** — re-running on the same day overwrites with the latest probable pitcher information without creating duplicates.

## Tables touched

All in the `propgpt_mlb` schema of the shared Neon DB:
- `teams` (bootstrap only — no-op after first run)
- `parks` (bootstrap only)
- `players` (stubs for probable pitchers; full enrichment happens later in `mlb_game_update`)
- `games`

## Operation modes

Selected by env var (first match wins):

| Mode | Env vars | Use case |
|---|---|---|
| Manual date range | `BACKFILL_START=YYYYMMDD` + `BACKFILL_END=YYYYMMDD` | Fill gaps after a multi-day cron outage |
| Auto rolling backfill | `BACKFILL_DAYS=N` | Refresh recent days (catches late lineup announcements) |
| Single date | `TARGET_DATE=YYYY-MM-DD` | Test a specific date |
| Default | (none) | Today (UTC) |

## Schedule

`.github/workflows/mlb_games_update.yml` runs at `0 11 * * *` UTC (6am ET).

Also supports `workflow_dispatch` from the GitHub UI — useful for backfill or after a code change.

## Required secrets

In repo Settings → Secrets and Variables → Actions:
- `DATABASE_URL` — Neon connection string

No API key required (MLB Stats API is free + public).

## Running manually (local)

```bash
cd mlb_games_update
pip install -r requirements.txt
cp .env.example .env  # edit and add your DATABASE_URL

# Today (default)
DATABASE_URL="postgresql://..." python -m mlb_games_update

# Specific date
DATABASE_URL="postgresql://..." TARGET_DATE="2026-05-14" python -m mlb_games_update

# Rolling 7-day backfill
DATABASE_URL="postgresql://..." BACKFILL_DAYS=7 python -m mlb_games_update

# Explicit date range
DATABASE_URL="postgresql://..." BACKFILL_START=20260501 BACKFILL_END=20260515 \
    python -m mlb_games_update
```

## Historical backfill behavior

The same code path serves the daily forward run and historical backfills, with a few backfill-aware behaviors:

- **Regular season only.** Games are filtered to `gameType == 'R'`; Spring Training, exhibitions, All-Star, and postseason games are dropped and the filtered count is logged (nothing disappears silently). This applies to the daily path too — we don't predict exhibition games.
- **Neutral-site venues.** Historical seasons include games at venues not in the seeded `parks` table (Seoul, London, Mexico City, Rickwood Field, Williamsport). Because `games.park_id` is a FK, each unknown venue is auto-stubbed into `parks` from `/venues/{id}` (id, name, coords, timezone — no park factors, so downstream treats it as league-average) and the game is kept. Each stub is logged.
- **Probable vs. actual starters.** Live/future games use the schedule's `probablePitcher`. Historical dates often have no probable, so games may land with NULL `home_sp_id`/`away_sp_id`; `mlb_results_update` then backfills the **actual** starters from the boxscore (the correct training-time value).
- **Politeness pacing.** In backfill mode (`BACKFILL_START/END` or `BACKFILL_DAYS`), a per-request sleep (`MLB_REQUEST_SLEEP_SEC`, default 0.4s) is enabled; the daily path stays unthrottled.
- **Resumability.** A failed date logs and continues; the run prints a list of failed dates at the end. Everything is idempotent, so any range can be safely re-run.

## Failure handling

- Exits 0 on success, 1 on failure
- GitHub Actions surfaces failures as red ❌ on the run
- All exceptions logged with full stack traces
- Re-running is always safe (idempotent upserts via ON CONFLICT DO UPDATE)

## Schema sync

The writer SQL here mirrors `propgpt-mlb/src/propgpt_mlb/ingestion/writers.py`. When the `propgpt_mlb` schema changes (new migration), update both files. Search for `SCHEMA SYNC` in `pipeline.py`.
