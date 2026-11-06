# CFB count-market integration

Adds passing attempts (`player_pass_attempts`), rushing carries (`player_rush_attempts`), and receptions (`player_receptions`) to collection, request validation, projection, public rendering and forward outcome tracking. Provider keys are documented at https://the-odds-api.com/sports-odds-data/betting-markets.html. Availability is per event/book; empty markets remain empty.

## Models and validation

`cfb.counts` trains separate nonnegative count estimators from existing `actual_volume` labels. A fixed 2023 selection compares a Poisson gradient-boosting estimator against the trailing-five volume baseline, requiring a 2% MAE improvement to replace the baseline. Models train through year N-2, calibrate on N-1 and evaluate on N. Calibration is count-specific: rounded, nonnegative empirical residual draws preserve integer pushes and never reuse yardage residuals. Missing category outcomes are not imputed as zero.

The existing participation estimator remains separate. Conditional probabilities describe a recorded category, not verified availability or a sportsbook DNP rule. The app's existing participation-aware grading path consumes the probabilities. No grade thresholds, per-game caps, or betting gates are changed in this integration.

Run explicitly, offline (no database writes):

```sh
python -m cfb.cli train-counts --features outputs/candidate_features_through_2026.csv --output models/saved/pou_v1
```

The output directory must contain the trusted yardage/participation release. The command adds three `*_volume` entries; it preserves yardage entries and their artifacts. Count estimators/calibration use content-addressed names, and the manifest is replaced only after every market finishes. Both estimator and count calibration checksums are verified at inference. Missing count artifacts fail explicitly; no fallback to yardage occurs. Re-running `train-release` creates a yardage-only manifest; run `train-counts` afterward when building a combined release.

Measured with the retained candidate feature file (SHA-256 `645532a5a8c092908eec9b682d494a4aa5653d90913d61001d17c00fe92bc019`):

| Market | Selected estimator | 2025 MAE / baseline | Available 2026 MAE / baseline | 2026 observations |
|---|---|---:|---:|---:|
| Pass attempts | Poisson boosting | 7.63 / 8.18 | 7.59 / 7.97 | 244 |
| Rush carries | Trailing-five mean | 3.87 / 3.87 | 4.07 / 4.07 | 830 |
| Receptions | Trailing-five mean | 1.34 / 1.34 | 1.38 / 1.38 | 1,344 |

The feature file predates this task and its 2026 sample is incomplete. These are conditional count errors, not betting returns. The 90% intervals cover 86.9%, 86.7%, and 87.9% of the available 2026 count outcomes respectively; calibration is imperfect. Synthetic-line Brier scores are diagnostics, not evidence of performance at historical sportsbook prices.

## Rollout

1. Deploy the collector/runtime source change; it requests six markets instead of three. Requests remain bounded by existing event limits, but provider credit usage can increase when new markets return.
2. Install the combined artifact bundle in the serving app, preserving the separate combo model directory.
3. Apply the matching app integration (supported stats, generator market map, count-aware rendering, artifact loading) while retaining its existing combination-analysis path.
4. Deploy the dashboard count settlement adapter, which reads `player_offense.volume` and retains pushes and missing outcomes correctly.
5. Verify new market candidates, grades and selected counts before considering selection-cap changes.

The app wrapper lives outside this repository. Its integration patch must accompany this release; updating this repository alone will not make new picks appear on the production board. Current combined-yardage serving is preserved; this change does not add new combo collection or touchdown/kicking markets.

A bounded live Odds API request for Alabama–LSU on November 6 returned 34 reception side/book quotes, with no attempts or carries quotes in that response. All three model paths also passed read-only inference checks against current player history. No production deployment or pick publication was performed by this integration task.
