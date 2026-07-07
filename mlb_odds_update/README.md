# mlb_odds_update

Intraday sportsbook odds snapshots for the MLB slate. Part of the PropGPT MLB prediction model pipeline.

## What it does

Several times a day, this folder's GitHub Actions workflow runs `python -m mlb_odds_update`, which:

1. Connects to the shared Neon database (via the `DATABASE_URL` secret).
2. Fetches **full-game** featured odds for `baseball_mlb` from The Odds API in **one bulk call** (`h2h`, `spreads`, `totals`).
3. Filters events to the target slate date (default: **today in US/Eastern** — MLB slates are ET-centric) and matches each event to a row in `propgpt_mlb.games` by date + home team + away team.
4. Upserts one `odds_snapshots` row per game × book × segment for the run's snapshot instant (`segment='full_game'`).
5. For each matched event, makes **one more call** to the per-event endpoint for **first-5-innings** markets (`h2h_1st_5_innings`, `spreads_1st_5_innings`, `totals_1st_5_innings`) and upserts those with `segment='f5'`.

Each run is a single snapshot in time; running multiple times a day is intentional — the rows form a per-game/book time series, and the latest snapshot before first pitch becomes the closing line for CLV (computed later).

## Tables touched

- `odds_snapshots` (in the `propgpt_mlb` schema)

Reads `teams` and `games` for event→game matching.

## Market → column mapping

Both segments share the `odds_snapshots` column shape:

| The Odds API market | Columns |
|---|---|
| `h2h` (moneyline) | `ml_home`, `ml_away` |
| `spreads` (run line) | `rl_home_spread`, `rl_home_odds`, `rl_away_odds` |
| `totals` | `total_line`, `over_odds`, `under_odds` |

`segment` (`full_game` / `f5`) and `odds_event_id` (The Odds API event id) were added in propgpt-mlb migration `006_odds_segments.sql`, which also adds the upsert key `UNIQUE (game_id, book, segment, snapshot_time)`.

## Bookmakers

Allowlist (config constant in `pipeline.py`): `draftkings`, `fanduel`, `betmgm`, `caesars`. All other books are ignored to control noise and credit cost.

> The Odds API still returns Caesars under the legacy `williamhill_us` key; the pipeline normalises it to `caesars` before the allowlist check and storage.

## Idempotency / snapshot behavior

Writes upsert on `UNIQUE (game_id, book, segment, snapshot_time)`. Within a single run all rows share one `snapshot_time` (run start, UTC), so re-inserting the same game/book/segment in that run updates rather than duplicates. Two separate runs use different `snapshot_time`s and therefore add new rows — this is the intended time series for CLV, not a duplication bug.

## Doubleheaders

If two games share date + teams, the event is matched to the game whose scheduled start is closest to the event's `commence_time`. If that can't be decided, the event is skipped with a warning rather than guessed.

## Operation modes

| Mode | Env vars | Use case |
|---|---|---|
| Single date | `TARGET_DATE=YYYY-MM-DD` (ET slate date) | Test / re-snapshot a specific date |
| Default | (none) | Today (US/Eastern) |

No historical backfill here — The Odds API historical endpoint is a separate later step.

## Schedule

`.github/workflows/mlb_odds_update.yml` runs at `0 12,16,20,23 * * *` UTC plus `0 1 * * *` UTC (four intraday snapshots plus one late-evening snapshot for West Coast night games; ~5 runs/day). Also supports `workflow_dispatch` with a `target_date` input.

## Required secrets

In repo Settings → Secrets and Variables → Actions:
- `DATABASE_URL` — Neon connection string
- `ODDS_API_KEY` — **must be added** (The Odds API v4 key)

## Cost awareness

Each request costs credits = markets × regions. One bulk full-game call (3 markets × 1 region = 3 credits) plus one F5 call per matched event (3 credits each). The `x-requests-remaining` / `x-requests-used` response headers are logged at the end of every run.

## Running manually (local)

```bash
cd mlb_odds_update
pip install -r requirements.txt
cp .env.example .env  # edit and add DATABASE_URL + ODDS_API_KEY

# Today (default, ET)
DATABASE_URL="postgresql://..." ODDS_API_KEY="..." python -m mlb_odds_update

# Specific date
DATABASE_URL="postgresql://..." ODDS_API_KEY="..." TARGET_DATE="2026-07-07" python -m mlb_odds_update
```

## Failure handling

- Exits 0 on success, 1 on failure.
- A failed F5 fetch or an unmatched event is logged and skipped — it never fails the run.
- Unresolvable API team names are logged loudly (`ERROR`) and surfaced in the run summary.
- Re-running is always safe (idempotent upserts).
