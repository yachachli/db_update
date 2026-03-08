# BracketIQ Model — How It Works

This document describes everything that goes into making the model function: data tables, programs, and the script that produces the daily output.

---

## 1. High-level flow

- **Inputs:** KenPom ratings (scraped), live or historical odds (The Odds API), optional historical results (FanMatch).
- **Core logic:** Team name resolution (Odds ↔ KenPom), margin/win-prob from AdjO/AdjD/AdjT, home court advantage, edges vs Vegas (spread, moneyline, over/under).
- **Daily output:** Today’s games with model vs Vegas edges → **`slate_today.json`** (produced by **`scripts/slate_today.py`**).

---

## 2. Data tables

All paths are relative to the **backend** directory unless noted. Config uses `app/config.py`: `CACHE_DIR` (default `app/data/cache`), `HISTORICAL_DIR` (default `app/data/historical`).

### 2.1 Static / rarely changed (curated)

| Table / file | Location | Purpose |
|--------------|----------|---------|
| **team_name_mapping.json** | `data/team_name_mapping.json` | Odds API name ↔ KenPom name. Used by odds_scraper and team_name_resolver. |
| **kenpom_aliases.json** | `data/kenpom_aliases.json` | Canonical KenPom name → list of alternate spellings (e.g. Tennessee Martin / UT Martin / Tenn-Martin). Used for cache lookups. |

**Update:** Edit by hand when new teams or naming quirks appear. Optionally run `py -m scripts.build_team_name_mapping` to infer from FanMatch + odds parquets (then review so built-in mappings aren’t overwritten).

---

### 2.2 KenPom cache (benefit from nightly update)

All under **`app/data/cache/`** (or `CACHE_DIR`). Written by **`scripts/refresh_kenpom_cache`** (calls `kenpom_scraper.refresh_all`). Latest file per pattern is used (e.g. newest `pomeroy_ratings_*_2026_*.parquet`).

| Table | Filename pattern | Used by |
|-------|------------------|---------|
| **Pomeroy ratings** | `pomeroy_ratings_*_2026_*.parquet` | slate_today, model_analysis, team_service, recency (AdjO, AdjD, AdjT, Rk, Team, Conf) |
| **Efficiency** | `efficiency_*_2026_*.parquet` | team_service (fallback) |
| **Four factors** | `fourfactors_*_2026_*.parquet` | model_analysis, team_service |
| **Team stats (off/def)** | `teamstats_off_*`, `teamstats_def_*` | model_analysis, team_service |
| **Height** | `height_*_2026_*.parquet` | model_analysis, team_service |
| **Point dist / KPOY / trends / arenas / HCA** | Various `*_*.parquet` | Optional / future use |
| **Player stats** | `playerstats_*_*.parquet` | Optional |
| **Game attributes** | `gameattribs_Excitement_*.parquet` | Optional |

**Update:** Run **`py -m scripts.refresh_kenpom_cache`** nightly (or as often as you want fresh ratings). Rate-limited (~8s+ between requests).

---

### 2.3 Historical data (benefit from nightly or periodic update)

Under **`data/historical/`** (or `HISTORICAL_DIR`).

| Table | Filename | Produced by | Purpose |
|-------|----------|-------------|---------|
| **FanMatch results** | `fanmatch_2026.parquet` | `scripts/collect_historical_fanmatch` | Game results, winner/loser, predicted winner, MOV. Source for schedules and ATS join. |
| **Historical odds** | `odds_2026.parquet` | `scripts/collect_historical_odds` | Historical spreads/totals (and team names) from The Odds API. |
| **ATS complete** | `ats_complete_2026.parquet` | `scripts/build_ats_dataset` | Join of FanMatch + historical odds: vegas_spread, actual_margin_home, covered_vegas, kenpom_vs_vegas_edge, etc. |

**Update:**  
- **FanMatch:** `py -m scripts.collect_historical_fanmatch` (nightly or after game days).  
- **Historical odds:** `py -m scripts.collect_historical_odds` (nightly or after updating FanMatch).  
- **ATS:** `py -m scripts.build_ats_dataset` after updating FanMatch and/or odds.

---

### 2.4 Analysis outputs (generated, not inputs)

Under **`data/analysis/`**.

| File | Produced by | Purpose |
|------|-------------|--------|
| **slate_today.json** | **`scripts/slate_today`** | **Daily model output:** today’s games with live odds + KenPom edges (spread, ML, O/U), sorted by edge. |
| daily_run_YYYYMMDD.json | `scripts/daily_run` | One day from ATS dataset, sorted by edge (example/presentation). |
| analysis_report.json | `scripts/model_analysis` | Summary stats (MAE, cover rates, etc.) from ATS + KenPom. |
| *.csv | `scripts/model_analysis` | Various analysis tables (conference, seed, etc.). |
| kenpom_team_names.txt | `scripts/export_kenpom_teams` | List of team names in Pomeroy cache (for mapping checks). |

---

## 3. Programs that make the model run

### 3.1 Scripts (run from `backend`: `py -m scripts.<name>`)

| Script | Role |
|--------|------|
| **refresh_kenpom_cache** | Scrapes KenPom and fills the cache (Pomeroy, efficiency, four factors, etc.). |
| **collect_historical_fanmatch** | Scrapes FanMatch by date → `fanmatch_2026.parquet`. |
| **collect_historical_odds** | Fetches historical odds for FanMatch dates → `odds_2026.parquet`. |
| **build_ats_dataset** | Joins FanMatch + historical odds → `ats_complete_2026.parquet`. |
| **build_team_name_mapping** | Infers Odds↔KenPom from parquets → `team_name_mapping.json` (optional). |
| **model_analysis** | Reads ATS + KenPom cache; writes analysis_report.json and CSVs. |
| **slate_today** | **Daily output:** fetches live odds, reads KenPom cache, computes edges → `slate_today.json` + stdout. |
| **daily_run** | One-day slice of ATS dataset by date, sorted by edge → `daily_run_YYYYMMDD.json`. |
| **verify_cover_rate** | Checks covered_vegas rate in ATS dataset. |
| **export_kenpom_teams** | Dumps KenPom team names from cache. |
| **audit_data** | Diagnostics on ATS / FanMatch / odds. |

### 3.2 App modules (used by scripts and API)

| Module | Role |
|--------|------|
| **app.config** | CACHE_DIR, HISTORICAL_DIR, env (KENPOM_*, ODDS_API_KEY). |
| **app.scrapers.odds_scraper** | The Odds API (live + historical), parse_game_odds, odds_to_kenpom_name, consensus lines. |
| **app.scrapers.kenpom_scraper** | KenPom login, refresh_all, get_cached_or_scrape. |
| **app.services.team_name_resolver** | resolve_odds_to_kenpom, resolve_to_canonical_kenpom, find_team_row, get_rating (uses aliases + mapping). |
| **app.services.schedule_service** | Load FanMatch parquet, parse game strings, reconstruct team schedules. |
| **app.services.team_service** | get_team_profile, list_teams, get_team_schedule_cached (KenPom cache + FanMatch). |
| **app.services.ats_service** | ATS stats by team from ats_complete (or FanMatch fallback). |
| **app.services.recency_service** | Recency metrics from schedule + KenPom ratings (uses resolver for lookups). |
| **app.services.matchup_service** | Today’s FanMatch / historical matchups, run_prediction. |

---

## 4. Daily output: the script and the file

- **Script:** **`scripts/slate_today.py`**  
  **Run:** `py -m scripts.slate_today` (from `backend`).

- **What it does:**  
  1. Fetches **live** NCAAB odds from The Odds API (spreads, totals, moneylines).  
  2. Loads the latest **Pomeroy ratings** from the KenPom cache.  
  3. For each game: resolves team names (Odds → KenPom via `team_name_resolver`), looks up AdjO/AdjD/AdjT, computes predicted margin and win prob, parses Vegas spread/total/implied prob.  
  4. Computes **spread edge**, **moneyline edge**, and **over/under edge** (model minus Vegas).  
  5. Builds JSON with games sorted by each edge type and an `all_games` list.  
  6. Prints JSON to stdout and writes **`data/analysis/slate_today.json`**.

- **Requires:**  
  - `ODDS_API_KEY` in `.env`.  
  - At least one **`pomeroy_ratings_*_2026_*.parquet`** in the cache (run `refresh_kenpom_cache` first).

- **Does not use:** FanMatch or historical odds parquets; it uses only live API + KenPom cache.

---

## 5. Summary: static vs nightly

| Type | Tables / files | Action |
|------|----------------|--------|
| **Static** | `team_name_mapping.json`, `kenpom_aliases.json` | Edit when needed; optional run of `build_team_name_mapping`. |
| **Nightly (or periodic)** | KenPom cache (`app/data/cache/*.parquet`) | `py -m scripts.refresh_kenpom_cache` |
| **Nightly (or periodic)** | `fanmatch_2026.parquet`, `odds_2026.parquet` | `collect_historical_fanmatch`, `collect_historical_odds` |
| **After historical update** | `ats_complete_2026.parquet` | `py -m scripts.build_ats_dataset` |
| **Daily output** | `slate_today.json` | **`py -m scripts.slate_today`** (after cache is updated). |

For “model up to date” and daily output, minimum is: **refresh_kenpom_cache** then **slate_today**. For full historical and analysis: also run FanMatch + odds collection, build_ats_dataset, and optionally model_analysis (see `RUNBOOK_UPDATE_MODEL.md`).
