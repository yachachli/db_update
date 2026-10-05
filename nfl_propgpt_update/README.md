# nfl_propgpt_update

Cron entry for the v2 NFL model (`zernerdoescode/nfl-propgpt-model`).
Writes `nfl_model_v2.*` on the shared Neon database.

The legacy `nfl-update.py` job is unchanged — it still refreshes `v3_nfl_*`.
This package is the new schema.

## Schedule

| UTC cron | Local (PT) | DAG |
| --- | --- | --- |
| `0 15 * * 2` | Tue 08:00 | box scores, grade, roster, tWAR, PGRS, DODES, T-120h |
| `0 15 * * 4` | Thu 08:00 | T-72h, injuries, early `predict_week`, odds |
| `0 15 * * 6` | Sat 08:00 | T-24h, final injuries, primary card |
| `0 14 * * 0` | Sun 07:00 | T-4h, props, lock predictions |

Manual run: Actions → **NFL PropGPT season ops** → Run workflow.

## Secrets on `yachachli/db_update`

| Secret | Used for |
| --- | --- |
| `NFL_MODEL_TOKEN` | PAT (or fine-grained token) with **read** on `zernerdoescode/nfl-propgpt-model` |
| `DATABASE_URL` or `DB_URL` | Neon URI (`?sslmode=require`). `DB_USER`/`DB_PASSWORD`/`DB_HOST`/`DB_NAME` also work |
| `TANK01_API_KEY` or `RAPIDAPI_KEY` | Tank01 / RapidAPI key |
| `DB_SCHEMA` | optional, default `nfl_model_v2` |

The same DAG also lives in the model repo (`.github/workflows/season-ops.yml`)
and can run there with only `TANK01_API_KEY` + `DB_URL`.
