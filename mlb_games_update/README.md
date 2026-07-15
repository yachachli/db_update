# mlb_games_update

MLB schedule + probable pitchers sync. Part of the PropGPT MLB prediction model pipeline.

## What it does

GitHub Actions runs `python -m mlb_games_update` several times per day. Each run:

1. Connects to the shared Neon database (via the `DATABASE_URL` secret)
2. On first run only: populates the 30 MLB teams and their home parks
3. Fetches the MLB slate from the free public MLB Stats API (`statsapi.mlb.com`) with `hydrate=probablePitcher`
4. Upserts each game (with probable pitchers when announced) into `propgpt_mlb.games`

The script is **idempotent** — re-running fills NULL `home_sp_id` / `away_sp_id` when MLB later posts a probable, without wiping an id that was already set (`COALESCE` on upsert).

## Tables touched

All in the `propgpt_mlb` schema of the shared Neon DB:
- `teams` (bootstrap only — no-op after first run)
- `parks` (bootstrap only)
- `players` (stubs for probable pitchers; handedness enrichment in this package)
- `games`

## Operation modes

Selected by env var (first match wins):

| Mode | Env vars | Use case |
|---|---|---|
| Manual date range | `BACKFILL_START=YYYYMMDD` + `BACKFILL_END=YYYYMMDD` | Fill gaps after a multi-day cron outage |
| Auto rolling backfill | `BACKFILL_DAYS=N` | Refresh recent days (ET “today”) |
| Single date | `TARGET_DATE=YYYY-MM-DD` | Test / force one slate date |
| Default (live) | (none) | **Today + tomorrow (US/Eastern)** — probable refresh |

## Schedule

`.github/workflows/mlb_games_update.yml` (UTC → approx ET during EDT):

| Cron (UTC) | ≈ ET | Purpose |
|---|---|---|
| `0 11 * * *` | 7am | Morning slate + overnight probables |
| `0 16 * * *` | 12pm | Late morning announcements / afternoon games |
| `0 20 * * *` | 4pm | Pre-evening first-pitch refresh |
| `0 23 * * *` | 7pm | West Coast / late changes + tomorrow listings |

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

# Today + tomorrow ET (default live mode)
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
