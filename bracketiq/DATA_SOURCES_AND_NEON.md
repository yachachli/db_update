# BracketIQ — Data Tables, Files, and Neon Port Checklist

Every datatable, CSV, and file that feeds the model, with **update frequency** and **Neon port status**. Use this to port everything that needs daily updating to Neon.

---

## All data files at a glance

| File | Location | Purpose | Daily? | Port to Neon? |
|------|----------|---------|--------|----------------|
| `pomeroy_ratings_*.parquet` | cache | KenPom ratings (AdjO/AdjD/AdjT) — core for slate & predictions | Yes | **Yes** → `kenpom_ratings` |
| `fanmatch_2026.parquet` | historical | KenPom FanMatch (spread/O/U source, recency schedules) | Yes (incremental + today) | Optional (slate uses it; ATS uses it) |
| `odds_2026.parquet` | historical | Historical Vegas spreads/totals by date | Yes (incremental) | Optional |
| `ats_complete_2026.parquet` | historical | Joined ATS outcomes (FanMatch + odds + results) | Yes (rebuilt after FM + odds) | **Yes** → `ats_historical` |
| `slate_today.json` | data/analysis | Today’s slate output (Vegas vs model, edges) | Yes | **Yes** → `slate_today` |
| `team_name_mapping.json` | data | Odds API → KenPom name map | Rare | No |
| `kenpom_aliases.json` | data | KenPom canonical name → aliases | Rare | No |
| `fourfactors_*.parquet` | cache | KenPom four factors | Yes | Optional |
| `teamstats_off_*.parquet` | cache | KenPom team stats (off) | Yes | Optional |
| `teamstats_def_*.parquet` | cache | KenPom team stats (def) | Yes | Optional |
| `height_*.parquet` | cache | KenPom height | Yes | Optional |
| `conference_accuracy.csv` | data/analysis | Conference MAE (model_analysis) | On demand | Optional |
| `edge_analysis.csv` | data/analysis | Edge cover rates (model_analysis) | On demand | Optional |
| Other analysis CSVs/JSON | data/analysis | Reports from model_analysis / daily_run | On demand | Optional |

---

## 1. Inputs the model reads (must exist for pipeline to work)

### 1.1 Cache directory (`app/data/cache` or `CACHE_DIR`)

| File / pattern | Purpose | Updated by | Update frequency | Port to Neon? |
|----------------|---------|------------|------------------|----------------|
| `pomeroy_ratings_*.parquet` | KenPom efficiency ratings (AdjO, AdjD, AdjT, etc.) — **core for slate & prediction** | `refresh_kenpom_cache` / KenPom scraper | Daily (nightly) | **Yes** — `kenpom_ratings` table |
| `efficiency_*.parquet` | KenPom efficiency (alternate/cache) | KenPom scraper | Daily | Optional (overlaps pomeroy) |
| `fourfactors_*.parquet` | Four factors (eFG, TO%, ORB, FTR) | KenPom scraper | Daily | Optional (team profiles, model_analysis) |
| `teamstats_off_*.parquet` | Team stats offense | KenPom scraper | Daily | Optional (team profiles, model_analysis) |
| `teamstats_def_*.parquet` | Team stats defense | KenPom scraper | Daily | Optional (team profiles, model_analysis) |
| `height_*.parquet` | Height/experience | KenPom scraper | Daily | Optional (model_analysis) |
| `pointdist_*.parquet` | Point distribution | KenPom scraper | Daily | Rarely used |
| `kpoy_*.parquet` | KPOY list | KenPom scraper | Daily | No |

**Critical for daily slate:** Only **`pomeroy_ratings_*.parquet`** is required for `slate_today` and the API prediction model. The rest are used by team profiles and `model_analysis`.

---

### 1.2 Historical directory (`app/data/historical` or `HISTORICAL_DIR`)

| File | Purpose | Updated by | Update frequency | Port to Neon? |
|------|---------|------------|------------------|----------------|
| `fanmatch_2026.parquet` | KenPom FanMatch scrapes (Game, PredictedMOV, PredictedWinner, results by date) — **spread/O/U from KenPom, recency schedules** | `collect_historical_fanmatch` (incl. `--today-only`) | Daily (incremental + today) | **Yes** — can be `fanmatch` or merged into slate source |
| `odds_2026.parquet` | Historical odds (spreads, totals by date/game) | `collect_historical_odds` | Daily (incremental) | **Yes** — for ATS backfill / analytics |
| `ats_complete_2026.parquet` | Joined FanMatch + odds + results (ATS outcomes, edges) | `build_ats_dataset` | After FanMatch + odds update | **Yes** — `ats_historical` table |

**Dependency order:**  
`fanmatch_2026` → (optional) `odds_2026` → `build_ats_dataset` → `ats_complete_2026`.

---

### 1.3 Config / mapping (static or rare edits)

| File | Purpose | Updated by | Update frequency | Port to Neon? |
|------|---------|------------|------------------|----------------|
| `data/team_name_mapping.json` | Odds API team name → KenPom name (explicit map) | Manual / `build_team_name_mapping` | Rare (when new names appear) | No (keep in repo or env) |
| `data/kenpom_aliases.json` | Canonical KenPom name → list of aliases (for resolver) | Manual | Rare | No (keep in repo) |

These are **not** daily data; keep in codebase or config. Neon does not need them.

---

### 1.4 Outputs written by the pipeline (derived)

| File | Purpose | Updated by | Update frequency | Port to Neon? |
|------|---------|------------|------------------|----------------|
| `data/analysis/slate_today.json` | Today’s slate: games, Vegas vs model, edges, best-value fields | `slate_today` | Daily (each run) | **Yes** — `slate_today` table (push_to_neon already does this) |

---

## 2. Analysis outputs (optional for “model working”)

Generated by `model_analysis` or other scripts; **not** required for the live slate or API. Port to Neon only if you want analytics in DB.

| File | Purpose | Updated by | Port to Neon? |
|------|---------|------------|----------------|
| `data/analysis/analysis_report.json` | Model analysis summary | `model_analysis` | Optional |
| `data/analysis/conference_accuracy.csv` | Conference MAE, etc. | `model_analysis` | Optional |
| `data/analysis/totals_correlations.csv` | Totals correlation stats | `model_analysis` | Optional |
| `data/analysis/spread_profiles.csv` | Spread profile analysis | `model_analysis` | Optional |
| `data/analysis/edge_analysis.csv` | Edge buckets, cover rates | `model_analysis` | Optional |
| `data/analysis/upset_patterns.csv` | Upset patterns | `model_analysis` | Optional |
| `data/analysis/venue_analysis.csv` | Venue analysis | `model_analysis` | Optional |
| `data/analysis/seed_simulation.csv` | Seed simulation | `model_analysis` | Optional |
| `data/analysis/daily_run_*.json` / `*.csv` | Daily run outputs | `daily_run` | Optional |
| `data/analysis/audit_report.txt` | Audit text | `audit_data` | No |
| `data/historical/scrape_progress.txt` | FanMatch scrape progress | `collect_historical_fanmatch` | No |

---

## 3. What to port to Neon for “daily updating”

Minimum set so the **model and slate** can run from Neon:

| Neon table / concept | Source file(s) | How to keep updated |
|---------------------|----------------|----------------------|
| **kenpom_ratings** | Latest `pomeroy_ratings_*.parquet` | Nightly: run KenPom refresh → `push_to_neon` (or write directly to Neon after scrape). |
| **slate_today** | `slate_today.json` (from `slate_today` script) | Nightly: run `slate_today` → `push_to_neon`. |
| **ats_historical** | `ats_complete_2026.parquet` | Nightly: after `collect_historical_fanmatch` + `collect_historical_odds` + `build_ats_dataset` → `push_to_neon`. |

Optional but useful for analytics and team features:

| Neon table / concept | Source file(s) | How to keep updated |
|----------------------|----------------|----------------------|
| **fanmatch** (if you add it) | `fanmatch_2026.parquet` | Nightly: after `collect_historical_fanmatch` → append/replace rows for new dates. |
| **odds** (if you add it) | `odds_2026.parquet` | Nightly: after `collect_historical_odds` → append/replace rows for new dates. |

Existing schema in `data/neon_schema.sql` already defines:

- `kenpom_ratings`
- `slate_today`
- `ats_historical`

`push_to_neon.py` currently loads from the **files** above and pushes these three tables. So “port everything that needs daily updating to Neon” is already satisfied for the **core** pipeline by:

1. Running the usual nightly jobs (KenPom refresh, FanMatch, odds, build_ats, slate_today).
2. Running `push_to_neon` at the end to sync **kenpom_ratings**, **slate_today**, and **ats_historical**.

If you add more tables (e.g. raw `fanmatch`, raw `odds`), add them to `neon_schema.sql` and to `push_to_neon.py` with the same pattern.

---

## 4. Quick reference: file → location

| File | Typical path (relative to backend) |
|------|-----------------------------------|
| Pomeroy cache | `app/data/cache/pomeroy_ratings_*.parquet` or `CACHE_DIR` |
| FanMatch | `app/data/historical/fanmatch_2026.parquet` or `HISTORICAL_DIR/fanmatch_2026.parquet` |
| Odds | `app/data/historical/odds_2026.parquet` |
| ATS complete | `app/data/historical/ats_complete_2026.parquet` |
| Slate output | `data/analysis/slate_today.json` |
| Team name mapping | `data/team_name_mapping.json` |
| KenPom aliases | `data/kenpom_aliases.json` |
| Neon schema | `data/neon_schema.sql` |

`CACHE_DIR` and `HISTORICAL_DIR` come from `app/config.py` (env or defaults).

---

## 5. Nightly update order (for Neon)

Recommended order so that by the time you run `push_to_neon`, all files are current:

1. **Refresh KenPom cache** — `refresh_kenpom_cache` (writes `pomeroy_ratings_*.parquet`, etc.)
2. **Collect FanMatch** — `collect_historical_fanmatch` or `--today-only` (writes/merges `fanmatch_2026.parquet`)
3. **Collect odds** — `collect_historical_odds` (writes/merges `odds_2026.parquet`)
4. **Build ATS** — `build_ats_dataset` (reads FanMatch + odds, writes `ats_complete_2026.parquet`)
5. **Build slate** — `slate_today` (reads cache + FanMatch + Odds API, writes `slate_today.json`)
6. **Push to Neon** — `push_to_neon` (reads pomeroy latest, `slate_today.json`, `ats_complete_2026.parquet`; writes `kenpom_ratings`, `slate_today`, `ats_historical`)

After step 6, all daily-updating data the model needs is in Neon.
