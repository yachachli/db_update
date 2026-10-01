# CFB POU v1: functional research model

Built September 30, 2026. The project now has reusable trained artifacts, a player/matchup/line analysis function, a CLI, and a tested local HTTP endpoint. It is **not yet a production betting recommender**. It returns conditional research projections and a model lean while keeping recommendation grades blocked where the required evidence is missing.

## Functional NFL POU comparison

The existing BestBet NFL implementation accepts player/team/opponent/stat/line inputs and returns a projection, direction, grade, historical support, graphs, and explanations. Its projection logic also uses injury/role context. This CFB adapter provides the analogous analysis shape for passing, rushing, and receiving yards:

- Exact CFBD game/player IDs, with optional team/opponent validation.
- Saved projection models, so a request does not retrain the supervised model.
- Opponent-adjusted pregame game/pass/rush context computed only from games before the as-of UTC date.
- Conditional empirical over/under/push estimates and a 90% interval.
- Historical over/under/push counts, a graph, and three deterministic explanations.
- Explicit unsupported-history and availability responses, rather than invented rookie or injury assumptions.

`model_lean` is the research direction. `over_under=null`, `grade=0`, and `recommendation_status="abstain"` mean there is **no authorized production recommendation**, not that a probability was secretly converted into a validated NFL-style grade. `injury=null` means unknown, not healthy. Boosted models target conditional median yardage; their volume/interval scaling uses trailing-five volume rather than a new workload regression.

The shared backend schema currently restricts `league` to NFL/NBA/MLB/WNBA. Adding CFB to that enum and routing this service would be a separate integration change. The existing backend and NFL projects were inspected read-only and were not modified, deployed, or given new routes. This is therefore NFL-POU-style, not an already integrated drop-in endpoint.

## Fixed model comparison

Candidates were fixed in advance: trailing-five mean, the existing two-stage volume/efficiency ridge, and a gradient-boosted median regressor. Selection used 2021–2022 training with 2023 validation, requiring at least a 2% MAE gain over the mean baseline before selecting a learned model. No selection used 2025 or 2026 results.

Selection chose volume/efficiency for passing and boosted median for rushing/receiving. The boosting settings are saved in Neon: 150 iterations, learning rate .05, 15 leaves, minimum leaf size 80, L2 10, absolute-error loss, fixed seed 42, no early-stopping split.

For 2025 evaluation, the selected models train through 2023 and calibrate on 2024. The reusable 2026 artifacts train through 2024 and calibrate on 2025. The small 2026 season-to-date evaluation was observed once in the original baseline refresh and then used for this fixed comparison; it was **not** used to retune or reselect candidates. Future changes must not describe it as untouched data again.

| Category | 2025 model / baseline MAE | 2026 model / baseline MAE | Graded 2026 rows |
| --- | ---: | ---: | ---: |
| Passing | 68.59 / 71.06 | 60.87 / 57.65 | 27 |
| Rushing | 25.86 / 27.53 | 25.73 / 27.70 | 85 |
| Receiving | 20.99 / 22.29 | 26.16 / 24.00 | 97 |

Rushing is the most promising candidate in this comparison. Passing and receiving do not beat the simple baseline in the small 2026 sample. These samples are too small for a strong reliability claim, especially with repeated players and games. Eligibility requires three earlier same-season FBS category appearances, so early-season coverage is narrow. No change was made to hide these losses or expand eligibility after inspecting them.

## Conditional probability checks

The estimator uses normalized signed calibration residuals, discretizes simulated yardage to integers, and accounts for pushes at integer lines. Probabilities sum to one. They are **conditional on a recorded category outcome** and are not claimed to be calibrated on all sportsbook participants.

For a repeatable diagnostic, each pregame reference line is `floor(trailing-five yardage mean)+0.5`. These are **synthetic reference lines**, not historical sportsbook quotes. No ROI is computed.

| Category | 2025 model / calibration-base-rate Brier | 2026 model / base-rate Brier |
| --- | ---: | ---: |
| Passing | .2369 / .2494 | .2691 / .2537 |
| Rushing | .2346 / .2457 | .2201 / .2458 |
| Receiving | .2354 / .2462 | .2819 / .2532 |

Lower is better. Reliability bins and interval coverage are preserved in the reports; no arbitrary high-confidence grade is derived from model/line distance. Actual sportsbook calibration, quote timestamps, settlement rules, and availability remain outstanding.

## Participation investigation

A deterministic 60-game development-data pilot reconciled play-level Reception/Target records against both individual receiving box scores and team completions/passing yards. Only 58 of 120 team-games passed; 47 failed team totals and 15 failed individual totals. The passing subset yielded 19 play-derived zero-catch records with targets. These are stored separately in `play_derived_zeros`, not inserted into the original player box scores or automatically added to model training. A re-audit can revoke this task's derived rows for a specific game/team if reconciliation fails; raw cached source responses remain available.

This is stronger evidence than assuming missing rows are zeros, but coverage is too inconsistent to fill every gap automatically. Model development cannot remove that uncertainty simply by repeating backtests. A reliable availability/participation source or reviewed manual process is still needed before betting-grade recommendations can be enabled.

## Run it

From the project root, with the configured Python environment and `.env`:

```sh
source .venv/bin/activate
python -m cfb.cli serve --port 8088
```

Send a request to the loopback-only endpoint in another terminal:

```sh
curl -X POST http://127.0.0.1:8088/cfb_pou \
  -H 'Content-Type: application/json' \
  --data-binary @examples/analysis_request.json
```

Or use the CLI directly:

```sh
python -m cfb.cli analyze --request '{"game_id":401871049,"player_id":4878394,"stat":"rush yards","line":49.5,"as_of":"2026-09-30T00:00:00Z"}'
```

The example's **49.5 line is hypothetical**, not a retrieved sportsbook quote. The smoke test returned Ajay Allen / Western Kentucky vs New Mexico State, a 46.30-yard projection, a research-only under lean, history and graph data, and an explicit abstention. Valid HTTP input returned 200; unsupported input returned 422. The temporary test server was stopped afterward; no persistent background service or cloud deployment was left running.

Supported `stat` values: `pass yards`, `rush yards`, `rec yards`. `as_of` defaults to current UTC and must precede kickoff and follow the saved calibration period. This bundle supports 2026 only. `team_code` and `opponent_abv`, if provided, must currently be exact CFBD school names; optional numeric `team_id` / `opponent_id` are checked. There is no fuzzy cross-provider player match. Historical replays use retrospective source data, not a point-in-time publication archive.

## Saved and tested

- Model artifacts and calibration residuals: `models/saved/pou_v1/` (ignored local artifacts; load only these trusted locally trained files).
- Point-model refresh: Neon runs 51–53. Fixed release validation: runs 54–59 and `release_predictions`.
- Release reports: `outputs/release_validation.json` and `outputs/run_54/` through `outputs/run_59/`.
- Play-data audit: `play_label_audits`, `play_derived_zeros`, and `outputs/play_label_pilot.json`.
- Thirty-five automated tests pass. Target-only inference rating calculations match the original full historical calculations. Existing NFL data/code and prior prediction runs remain untouched.

Rebuild commands are `python -m cfb.cli train-release` after generating `outputs/candidate_features_through_2026.csv` with the POU history/backtest pipeline. Tests: `python -m unittest discover -s tests -v`. The dedicated `.venv` is installed with the model-library versions in `constraints-runtime.txt`; rebuilding it uses Python 3.12 and `pip install -e . -c constraints-runtime.txt`. Production still needs service hardening/authentication, integration tests against the CFB-extended backend schema, availability/label resolution, and validated market data. No model is promised to be profitable.
