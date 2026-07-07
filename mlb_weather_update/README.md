# mlb_weather_update

Game-time weather forecasts for the MLB slate. Part of the PropGPT MLB prediction model pipeline.

## What it does

A couple of times a day, this folder's GitHub Actions workflow runs `python -m mlb_weather_update`, which:

1. Connects to the shared Neon database (via the `DATABASE_URL` secret).
2. Loads today's games (US/Eastern slate date; `TARGET_DATE` env override) joined to `propgpt_mlb.parks` for coordinates, timezone, and dome/roof flags.
3. For each game, selects the hourly forecast bucket closest to first pitch (in UTC) from [Open-Meteo](https://open-meteo.com/) — free, no API key.
4. Upserts one row per game into `propgpt_mlb.weather_observations`.

## Weather source

- Forecast endpoint (used here): `https://api.open-meteo.com/v1/forecast`
- Historical endpoint (later backfill, **not used in this step**): `https://archive-api.open-meteo.com/v1/archive`

Both share the same hourly-variable shape, so `open_meteo_client.py` takes a parameterizable base URL + endpoint. Hourly variables requested: `temperature_2m, relative_humidity_2m, apparent_temperature, precipitation_probability, precipitation, wind_speed_10m, wind_direction_10m, wind_gusts_10m, cloud_cover` (units: °F, mph, `timezone=UTC`).

## Dome / roof handling

| Park type (`parks` flags) | Behavior |
|---|---|
| Fixed dome (`is_dome = true`) | Row written with `is_dome_game = true` and all weather fields NULL — weather features go neutral downstream. No API call. |
| Retractable roof (`has_retractable_roof = true`) | Real forecast fetched and stored with `is_dome_game = false`. Roof status at game time is unknown and `weather_observations` has no retractable flag, so the stored weather is the open-air forecast (limitation noted here). |
| Open-air | Real forecast fetched and stored. |

## Field mapping (Open-Meteo hourly → `weather_observations`)

| Column | Source |
|---|---|
| `observed_for_time` | selected forecast hour (UTC) |
| `pulled_at` | run time (`NOW()`) |
| `source` | `'open-meteo'` |
| `temp_f` | `temperature_2m` |
| `feels_like_f` | `apparent_temperature` |
| `wind_mph` | `wind_speed_10m` |
| `wind_dir_deg` | `wind_direction_10m` |
| `precip_pct` | `precipitation_probability` |
| `humidity_pct` | `relative_humidity_2m` |
| `cloud_cover_pct` | `cloud_cover` |
| `is_dome_game` | fixed-dome flag |

**Not stored (no column):** `wind_gusts_10m` (wind gusts) and `precipitation` (amount). The gust value in particular could be useful for the model; a future migration could add `wind_gust_mph NUMERIC(4,1)` — not applied here.

## Game-hour selection

Each game's `games.game_time_utc` is the scheduled first pitch. The hourly forecast bucket closest to that time (UTC) is chosen. If a game has no start time, the run falls back to 7pm local park time and logs a warning.

## Upsert semantics

`weather_observations.game_id` is the PRIMARY KEY, so writes upsert `ON CONFLICT (game_id) DO UPDATE`: one row per game, later runs overwrite with a fresher forecast (`pulled_at` refreshed). Unlike odds, weather is not kept as a time series — no migration was needed.

## Tables touched

- `weather_observations` (in the `propgpt_mlb` schema). Reads `games` + `parks`.

## Operation modes

| Mode | Env vars | Use case |
|---|---|---|
| Single date | `TARGET_DATE=YYYY-MM-DD` (ET slate date) | Test / refresh a specific date |
| Default | (none) | Today (US/Eastern) |

## Schedule

`.github/workflows/mlb_weather_update.yml` runs at `30 12 * * *` UTC (morning, after the slate sync) and `0 21 * * *` UTC (evening refresh before first pitches). Also supports `workflow_dispatch` with a `target_date` input.

## Required secrets

- `DATABASE_URL` — Neon connection string. No other secrets (Open-Meteo is keyless).

## Running manually (local)

```bash
cd mlb_weather_update
pip install -r requirements.txt
cp .env.example .env  # edit and add DATABASE_URL

DATABASE_URL="postgresql://..." python -m mlb_weather_update
DATABASE_URL="postgresql://..." TARGET_DATE="2026-07-07" python -m mlb_weather_update
```

## Failure handling

- Exits 0 on success, 1 on fatal error (e.g. missing `DATABASE_URL`).
- Per-game failures (bad coords, forecast fetch error) log a warning and continue — one bad game never fails the run.
- Re-running is always safe (idempotent game-level upsert).
