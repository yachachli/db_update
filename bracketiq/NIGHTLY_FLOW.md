# BracketIQ Nightly Update — Flow

Runs **daily at 8 AM UTC** (3 AM EST) via GitHub Actions. This diagram shows how data moves between Neon, local files, and external APIs.

```mermaid
flowchart TB
    subgraph neon["Neon (PostgreSQL)"]
        n_fanmatch[(bracketiq_fanmatch_historical)]
        n_odds[(bracketiq_odds_historical)]
        n_ats[(bracketiq_ats_historical)]
        n_kenpom[(bracketiq_kenpom_* 5 tables)]
        n_slate[(bracketiq_slate_today)]
    end

    subgraph local["Local (CI runner)"]
        subgraph hist["app/data/historical"]
            h_fanmatch[fanmatch_2026.parquet]
            h_odds[odds_2026.parquet]
            h_ats[ats_complete_2026.parquet]
        end
        subgraph cache["app/data/cache"]
            c_pomeroy[pomeroy_ratings_*.parquet]
            c_ff[fourfactors_*.parquet]
            c_ts[teamstats_off/def, height_*.parquet]
        end
        slate_json[data/analysis/slate_today.json]
    end

    subgraph external["External"]
        kenpom_site[KenPom.com]
        odds_api[Odds API]
    end

    %% 1. Pull from Neon
    n_fanmatch -->|"1. Pull"| h_fanmatch
    n_odds -->|"1. Pull"| h_odds
    n_ats -->|"1. Pull"| h_ats

    %% 2. Refresh KenPom
    kenpom_site -->|"2. Refresh KenPom"| c_pomeroy
    kenpom_site -->|"2. Refresh KenPom"| c_ff
    kenpom_site -->|"2. Refresh KenPom"| c_ts

    %% 3. Collect FanMatch (incremental)
    kenpom_site -->|"3. Collect FanMatch"| h_fanmatch

    %% 4. Collect Odds (incremental)
    odds_api -->|"4. Collect Odds"| h_odds

    %% 5. Build ATS
    h_fanmatch -->|"5. Build ATS"| h_ats
    h_odds -->|"5. Build ATS"| h_ats

    %% 6. Generate Slate
    c_pomeroy -->|"6. Generate Slate"| slate_json
    h_fanmatch -->|"6. today's FanMatch"| slate_json
    odds_api -->|"6. live odds"| slate_json

    %% 7. Push to Neon
    c_pomeroy -->|"7. Push"| n_kenpom
    c_ff -->|"7. Push"| n_kenpom
    c_ts -->|"7. Push"| n_kenpom
    slate_json -->|"7. Push"| n_slate
    h_fanmatch -->|"7. Push"| n_fanmatch
    h_odds -->|"7. Push"| n_odds
    h_ats -->|"7. Push"| n_ats
```

## Step summary

| Step | Script | Reads | Writes |
|------|--------|--------|--------|
| **1** | `pull_from_neon` | Neon: fanmatch, odds, ats | `app/data/historical/*.parquet` |
| **2** | `refresh_kenpom_cache` | KenPom.com | `app/data/cache/*.parquet` (pomeroy, fourfactors, teamstats, height) |
| **3** | `collect_historical_fanmatch` | KenPom.com | `fanmatch_2026.parquet` (incremental) |
| **4** | `collect_historical_odds` | Odds API | `odds_2026.parquet` (incremental) |
| **5** | `build_ats_dataset` | fanmatch + odds parquets | `ats_complete_2026.parquet` |
| **6** | `slate_today` | cache (pomeroy), fanmatch (today), Odds API (live) | `data/analysis/slate_today.json` |
| **7** | `push_to_neon` | cache + slate_today.json + historical parquets | All `bracketiq_*` tables in Neon |

## Neon tables (after push)

- **KenPom:** `bracketiq_kenpom_ratings`, `bracketiq_kenpom_fourfactors`, `bracketiq_kenpom_teamstats_off`, `bracketiq_kenpom_teamstats_def`, `bracketiq_kenpom_height`
- **Slate:** `bracketiq_slate_today`
- **Historical:** `bracketiq_fanmatch_historical`, `bracketiq_odds_historical`, `bracketiq_ats_historical`

The consumer app (BracketIQ API) reads only from these Neon tables; it does not use local parquets or run these scripts.
