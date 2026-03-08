# BracketIQ — Export to db_update Repo

Use this checklist to add BracketIQ as a `bracketiq/` folder inside **yachachli/db_update**, matching the NBA folder pattern. PropGPT reads from Neon; no manual SQL needed (push_to_neon creates tables).

**Workflow secret:** The YAML uses **`secrets.DB_URL`** to match the repo (per NBA "Use DB_URL and RAPIDAPI_KEY"). Confirm in repo Settings → Secrets that `DB_URL` exists; if you use `NEON_DATABASE_URL` instead, change those two steps in the workflow.

---

## 1. Create `bracketiq/` in db_update

Copy the **contents** of this backend folder into `db_update/bracketiq/` so you get:

```
db_update/
├── .github/workflows/
│   ├── bracketiq_nightly.yml   ← full nightly run
│   ├── bracketiq_kenpom.yml    ← manual: KenPom only
│   ├── bracketiq_historical.yml ← manual: historical only
│   └── bracketiq_slate.yml     ← manual: slate only
├── bracketiq/
│   ├── __init__.py
│   ├── scripts/
│   │   ├── __init__.py
│   │   ├── refresh_kenpom_cache.py
│   │   ├── collect_historical_fanmatch.py
│   │   ├── collect_historical_odds.py
│   │   ├── build_ats_dataset.py
│   │   ├── slate_today.py
│   │   ├── push_to_neon.py
│   │   └── pull_from_neon.py
│   ├── app/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── scrapers/ (__init__.py, kenpom_scraper.py, odds_scraper.py)
│   │   └── services/ (__init__.py, team_name_resolver.py)
│   ├── data/
│   │   ├── team_name_mapping.json
│   │   └── kenpom_aliases.json
│   └── requirements.txt
```

**Do not copy** (generated at runtime):

- `app/data/cache/*.parquet`
- `app/data/historical/*.parquet`
- `data/analysis/*.json`, `*.csv`, `*.txt`

---

## 2. Imports and paths

- **Scripts** already use `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` so `from app.config` works when run from `bracketiq/` (parent of `scripts/` = `bracketiq`).
- **config.py** defaults `CACHE_DIR` and `HISTORICAL_DIR` to paths under the folder containing `app/` (`_BASE_DIR`), so they work when the repo root is db_update and `working-directory: bracketiq` is set.

No import renames to `bracketiq.app` are required if you run with `working-directory: bracketiq` and keep the `app` package under `bracketiq/`.

---

## 3. Table prefix

All BracketIQ tables use the **`bracketiq_`** prefix so they don’t clash with NBA tables in the same Neon DB:

| Table | Content |
|-------|--------|
| `bracketiq_kenpom_ratings` | Pomeroy ratings (~365) |
| `bracketiq_kenpom_fourfactors` | Four factors |
| `bracketiq_kenpom_teamstats_off` / `_def` | Team stats |
| `bracketiq_kenpom_height` | Height/experience |
| `bracketiq_slate_today` | Today’s slate + edges |
| `bracketiq_ats_historical` | ATS results (~4k) |
| `bracketiq_fanmatch_historical` | FanMatch history (~5.6k) |
| `bracketiq_odds_historical` | Historical odds (~5.2k) |

PropGPT queries e.g. `SELECT * FROM bracketiq_slate_today ORDER BY abs(spread_edge) DESC`.

---

## 4. GitHub Actions

- Copy **`backend/.github/workflows/bracketiq_nightly.yml`** to **`db_update/.github/workflows/bracketiq_nightly.yml`**.
- Copy the **manual per-table-group** workflows so you can run and verify each part separately:
  - **`bracketiq_kenpom.yml`** — KenPom refresh + push (5 tables).
  - **`bracketiq_historical.yml`** — Pull → FanMatch → Odds → ATS → push historical (3 tables).
  - **`bracketiq_slate.yml`** — Pull → KenPom refresh → Slate → push slate (1 table).
- All workflows use `defaults.run.working-directory: bracketiq`, so steps run from `db_update/bracketiq/`.

---

## 5. Secrets (db_update repo)

The workflow is set to use **`DB_URL`** (same as the NBA folders). In **Settings → Secrets and variables → Actions** ensure you have:

| Secret | Value |
|--------|--------|
| `KENPOM_EMAIL` | KenPom email |
| `KENPOM_PASSWORD` | KenPom password |
| `ODDS_API_KEY` | Odds API key |
| `DB_URL` | Neon connection string |

The workflow passes `secrets.DB_URL` as `NEON_DATABASE_URL` env var; the scripts accept either name. If your repo uses a secret named `NEON_DATABASE_URL` instead, edit the workflow and change `secrets.DB_URL` → `secrets.NEON_DATABASE_URL` in the Pull and Push steps.

---

## 6. .gitignore (db_update repo)

Add to the **root** `.gitignore` of db_update:

```gitignore
# BracketIQ generated data
bracketiq/app/data/cache/*.parquet
bracketiq/app/data/historical/*.parquet
bracketiq/data/analysis/*.json
bracketiq/data/analysis/*.csv
bracketiq/data/analysis/*.txt
```

---

## 7. Local test (from db_update clone)

```bash
cd db_update/bracketiq

export NEON_DATABASE_URL="your_connection_string"
export KENPOM_EMAIL="your_email"
export KENPOM_PASSWORD="your_password"
export ODDS_API_KEY="your_key"

python -m scripts.pull_from_neon
python -m scripts.refresh_kenpom_cache
python -m scripts.collect_historical_fanmatch
python -m scripts.collect_historical_odds
python -m scripts.build_ats_dataset
python -m scripts.slate_today
python -m scripts.push_to_neon
```

After `push_to_neon`, all 9 `bracketiq_*` tables exist in Neon with data. No manual SQL in the Neon console is required.

---

## 8. Verify in Neon

```python
import os
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.environ["NEON_DATABASE_URL"], connect_args={"sslmode": "require"})

for table in ["bracketiq_kenpom_ratings", "bracketiq_kenpom_fourfactors", "bracketiq_kenpom_height",
              "bracketiq_slate_today", "bracketiq_ats_historical", "bracketiq_fanmatch_historical",
              "bracketiq_odds_historical"]:
    try:
        n = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table}", engine).iloc[0]["n"]
        print(f"  {table}: {n} rows")
    except Exception as e:
        print(f"  {table}: {e}")
```

Expected ballpark: kenpom tables ~365, slate_today 20–60, ats ~4k, fanmatch ~5.6k, odds ~5.2k.

---

## 9. Copy order and first run

1. **Copy backend → db_update/bracketiq**  
   Copy everything under `backend/` into `db_update/bracketiq/` (scripts, app, data with the two JSONs, requirements.txt). Do **not** copy `app/data/`, `data/analysis/` generated files, or `.env`.

2. **Copy workflows**  
   Copy all four from `backend/.github/workflows/` to `db_update/.github/workflows/`: `bracketiq_nightly.yml`, `bracketiq_kenpom.yml`, `bracketiq_historical.yml`, `bracketiq_slate.yml`.

3. **.gitignore**  
   Add the BracketIQ entries from section 6 to the root `.gitignore` of db_update.

4. **Secrets**  
   Ensure `DB_URL`, `KENPOM_EMAIL`, `KENPOM_PASSWORD`, `ODDS_API_KEY` exist in the repo (DB_URL is used by the workflow).

5. **Push and trigger**  
   Commit, push, then go to the Actions tab and run **BracketIQ Nightly Update** via **Run workflow**. The Push step should list all 9 `bracketiq_*` tables with row counts. After that, BracketIQ is live in Neon and PropGPT can read from it.
