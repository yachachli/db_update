# Deployment boundary and operations

This is a **research deployment**, not a betting-ready launch. POU serving and
conditional forward tracking work; game predictions have a backtesting CLI but
not yet a live game-serving API. Availability, definitive sportsbook identity,
book grading rules, and demonstrated out-of-sample market performance are still
unresolved. All recommendation gates stay closed. Never automatically retrain or
promote a model in a data-update cron.

## Repository layout

- `zernerdoescode/cfb-game-predictions`: game-model baseline and efficiency research.
- `zernerdoescode/cfb-pou`: POU research/serving plus the shared data runtime.
- `yachachli/db_update`: the sole scheduler, with additive `cfb_runtime/` and
  `.github/workflows/cfb-update.yml`. Existing sports jobs are unchanged.

Install the two model repositories into separate Python 3.12 environments.
They intentionally share versioned core source snapshots (`cfb` imports), not a
mutable cross-repository runtime dependency. Source manifests fingerprint the
exports. Changes to shared code must be synchronized and both suites rerun.
The db_update copy is a tested snapshot of POU, not a checkout of a moving branch.

## Secrets and activation

Use these db_update Actions secrets (existing shared values are reused without changes):

- `DATABASE_URL`: the existing Neon database, with CFB writes restricted to `cfb_model_v1`
  where practical. Never print the URL. Other sports' DB secrets remain untouched.
- `CFBD_API_KEY`: the existing authenticated CFBD key.
- `ODDS_API_KEY`: the existing shared Odds API key. CFB calls consume its shared quota.

The workflow is scheduled but gated by repository variable `CFB_CRON_ENABLED=true`.
Leave it disabled until the PR is reviewed, secrets are configured, and manual
weekly and markets runs both pass. Push access does not imply permission to set
repository secrets or variables. Do not merge or enable a cron with missing secrets.

Proposed schedules, all UTC:

| Job | Time | Purpose |
| --- | --- | --- |
| Weekly refresh/catch-up | Sun/Mon/Wed/Fri 10:17 | Schedule, last 3 completed week partitions of team/player stats, coverage, outcomes |
| Market capture/audit | Tue–Sat 16:37 | Schedule, coverage, up to 10 games' props, up to 7 games' roster evidence |

The weekly runner name describes its data window; multiple runs catch corrections
and weekday games. There is no persistent local laptop dependency. GitHub Actions
can delay scheduled jobs; these are snapshots, not guaranteed closing prices.
One concurrency group plus a Neon transaction advisory lock prevents pipeline
overlap. Manual ingest commands bypass that lock; do not run them concurrently.
The workflow times out after 30 minutes. A killed job can leave a `running` ledger
row; inspect Actions before treating that row as active. Never delete its history.

## CLI / recovery

```sh
python -m pip install -e . -c constraints-runtime.txt
python -m unittest discover -s tests
python -m cfb.cli migrate
python -m cfb.cli pipeline --mode weekly --dry-run
python -m cfb.cli pipeline --mode weekly --lookback 3
python -m cfb.cli pipeline --mode markets --analyses 0
```

`pipeline_runs` records stage, status, timestamps, safe exception class and coverage.
A failed stage stops downstream execution. Re-run after fixing the cause; source
stats upsert and prior quote/prediction/outcome evidence remains intact. Recent
partitions are ordered by completed kickoff, not numerical week, so January
postseason week 1 follows December regular-season weeks. January–June defaults
to the preceding fall season; override `--season` deliberately for backfills.
All API calls retain bounded retries/timeouts; no purchases or account upgrades.
Fresh weekly data bypasses the local HTTP cache. Advanced-stat experiments are
not in this cron because the current released POU uses score and box-score features.

The weekly window is not a cold bootstrap. For a new database, first ingest at
least two prior seasons and the current season's games, team stats and player
history, using their explicit-year CLI commands. Current-season coverage and
minimum historical schedule counts are checked before downstream work. For a
long outage, do a full-season backfill before restarting the bounded cron. Player
category absence is not zero; coverage only confirms game/team source presence.

## Frozen POU artifact deployment

The repository does not commit binary models, raw caches, credentials or outputs.
A private release asset `cfb-pou-v2-models.zip` plus its SHA-256 sidecar contains
all ten trusted yardage/participation model, manifest and calibration files. Download both from the
approved private release, verify the archive hash, inspect its paths, and extract
under the checked-out POU repository. Never load untrusted joblib/pickle artifacts.
Do not install the game and POU packages into the same environment.

On a trusted runtime with those artifacts, `pipeline --mode markets --analyses 10`
can freeze up to one new conditional research response per game. The initial
db_update workflow explicitly uses **zero analyses** until artifact deployment is
reviewed; it maintains stats, quotes, identity evidence and outcome tracking.
It does not silently pretend that a fresh checkout has trained models. Artifact
season is validated before inference. Version 2 supports the 2026 season only.

## Release checklist

- Unit suites and secret scan clean in both exports.
- No `.env`, API values, database URLs with credentials, caches or raw data tracked.
- Private repositories and CI confirmed; source manifests committed.
- db_update PR reviewed; DATABASE_URL, ODDS_API_KEY and CFBD_API_KEY secrets are present.
- Both manual modes succeed; inspect Neon coverage and run status.
- Activate the cron variable only after those checks.
- Deploy trusted artifacts separately before enabling scheduled inference.
- Do not expose the unauthenticated localhost POU prototype to the public internet.
- Production recommendation/availability/settlement validation remains a separate gate.

GitHub scheduling reference: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule
