# BracketIQ — Bring the model up to date

Run all commands from the **backend** directory. Ensure `.env` has:

- `KENPOM_EMAIL` / `KENPOM_PASSWORD` (for KenPom scrape)
- `ODDS_API_KEY` (for live and historical odds)

---

## One-time / first-time setup

```powershell
cd "c:\Users\elile\OneDrive\Desktop\website college basketball model\backend"
# Activate venv if you use one (e.g. .venv\Scripts\Activate.ps1)
```

---

## Full update (run in this order)

### 1. Refresh KenPom cache (ratings, Pomeroy, four factors, etc.)

Required for: slate_today, model_analysis, team profiles, recency.  
Roughly 8+ seconds between each table; total ~5–15 minutes.

```powershell
py -m scripts.refresh_kenpom_cache
```

### 2. (Optional) Refresh FanMatch historical data

Only if you want the latest FanMatch results in the ATS dataset.  
Uses KenPom login; long run (10s per day).

```powershell
py -m scripts.collect_historical_fanmatch
```

### 3. (Optional) Refresh historical odds

Only if you updated FanMatch or want newer historical lines.  
Uses Odds API; 1s between dates.

```powershell
py -m scripts.collect_historical_odds
```

### 4. Rebuild ATS dataset

Run after updating FanMatch and/or historical odds.  
Joins `fanmatch_2026.parquet` + `odds_2026.parquet` → `ats_complete_2026.parquet`.

```powershell
py -m scripts.build_ats_dataset
```

### 5. Run model analysis (optional)

Uses `ats_complete_2026.parquet` + KenPom cache.  
Writes CSVs and `analysis_report.json` to `data/analysis/`.

```powershell
py -m scripts.model_analysis
```

### 6. Get today’s slate with edges (live)

Uses **live** Odds API + KenPom cache. No historical parquets required.  
Output: `data/analysis/slate_today.json` and printed JSON.

```powershell
py -m scripts.slate_today
```

---

## Quick “minimal” update (no historical re-scrape)

If you only want **today’s games** and **current KenPom ratings**:

```powershell
cd "c:\Users\elile\OneDrive\Desktop\website college basketball model\backend"
py -m scripts.refresh_kenpom_cache
py -m scripts.slate_today
```

---

## Full “everything” update (copy‑paste block)

Run from `backend`:

```powershell
py -m scripts.refresh_kenpom_cache
py -m scripts.collect_historical_fanmatch
py -m scripts.collect_historical_odds
py -m scripts.build_ats_dataset
py -m scripts.model_analysis
py -m scripts.slate_today
```

---

## Verification (optional)

- **Cover rate check** (ATS dataset):  
  `py -m scripts.verify_cover_rate`  
  Optionally: `py -m scripts.verify_cover_rate --diagnose`

- **Export KenPom team names** (to compare with mappings):  
  `py -m scripts.export_kenpom_teams`

- **Data audit**:  
  `py -m scripts.audit_data`

---

## Output locations

| Step                    | Main output |
|-------------------------|-------------|
| refresh_kenpom_cache    | `app/data/cache/` (pomeroy_ratings_*.parquet, etc.) |
| collect_historical_fanmatch | `data/historical/fanmatch_2026.parquet` |
| collect_historical_odds | `data/historical/odds_2026.parquet` |
| build_ats_dataset       | `data/historical/ats_complete_2026.parquet` |
| model_analysis          | `data/analysis/*.csv`, `analysis_report.json` |
| slate_today             | `data/analysis/slate_today.json` |
