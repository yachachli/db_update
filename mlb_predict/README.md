# mlb_predict — daily MLB game predictions

Runs the PropGPT-MLB totals (Ridge) + moneyline (Logit) models against the
`propgpt_mlb` schema and upserts one row per game into
`propgpt_mlb.predictions`. Served to the app by bestbet_backend's
`fetch_mlb_game_predictions`; the daily_task `generate_mlb_analysis` job
upgrades each row's explanation to GPT text afterward.

## SCHEMA / CODE SYNC

**Source of truth is the `propgpt-mlb` repo** (model training, migrations,
feature engineering). This folder is a vendored copy — the same pattern as
`wc2026/` — because GitHub Actions runs on this repo's account. When the model
or feature code changes upstream, re-sync:

```bash
cp -r ../propgpt-mlb/src/propgpt_mlb mlb_predict/propgpt_mlb
cp ../propgpt-mlb/scripts/predict_slate.py mlb_predict/predict_slate.py
#   ...then re-apply the path tweaks at the top of predict_slate.py
#   (MODELS_DIR / out_dir use `parent`, not `parent.parent`)
cp ../propgpt-mlb/models/*.joblib mlb_predict/models/
```

Retraining happens in propgpt-mlb (`scripts/backtest.py --walk-forward
--save-models`); copy the fresh `.joblib` files here afterward.

## Schedule

`.github/workflows/mlb_predict_slate.yml` — 13:00 UTC (after the 11:00 games /
12:00 odds / 12:30 weather ingests) predicting today ET + tomorrow ET, and a
20:30 UTC refresh for late pitcher changes. Off-days exit 0 with a warning.

## Local run

```bash
cd mlb_predict
pip install -r requirements.txt
DATABASE_URL=postgresql://... python predict_slate.py --date 2026-07-20 --write-db
```
