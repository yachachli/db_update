# Conditional player-yardage intervals

Completed September 30, 2026. `pou-interval-v0` adds nominal 80% and 90% residual intervals to frozen player-yardage models, plus explicit abstention rules. These are research intervals conditional on recorded category outcomes—not sportsbook probabilities or individual coverage guarantees.

## Results

2025 coverage among explicit category outcomes, before research filtering:

| Category | Fixed-width 90% coverage | Workload-scaled 90% coverage | Fixed / scaled mean full width (yards) |
| --- | ---: | ---: | ---: |
| Passing | 88.50% | 88.33% | 273.35 / 278.44 |
| Rushing | 91.01% | 90.42% | 112.34 / 104.48 |
| Receiving | 90.40% | 90.31% | 91.44 / 82.47 |

Passing undercovers the nominal 90% target in 2025 under both specifications; the bands are also broad. Workload scaling reduces mean interval width for rushing and receiving but not passing in this comparison. No method was retuned on these test results, and no significance test or independent final validation has been performed.

At the nominal 80% level, workload-scaled 2025 coverage is 78.58% passing, 80.88% rushing, and 79.88% receiving. Workload-scaled 90% coverage in 2024 was 91.45%, 90.57%, and 90.11%, respectively. Receiving remains excluded from research eligibility regardless of this conditional coverage because its missing-zero sensitivity remains unresolved.

## Chronological design

| Evaluation season | Supervised point-model training through | Calibration season |
| --- | --- | --- |
| 2024 | 2022 | 2023 |
| 2025 | 2023 | 2024 |

The calibration model is frozen and reused on the next season's feature rows. The newer model originally attached to those next-season rows is **not** used. The saved model must reproduce its original calibration predictions before the evaluation can proceed. Last calibration-game kickoff must precede the first test cutoff. No test label enters calibration or abstention thresholds. Pregame history features still update with prior games as they would operationally.

Calibration uses the `ceil((n+1)*level)`-th ordered residual score, requiring at least 100 explicit calibration outcomes. Fixed-width scores are absolute yardage residuals. Scaled scores divide those residuals by `sqrt(max(projected_volume,1))`; actual test workload is never used. Both variants use symmetric intervals, preserving negative lower bounds because negative yardage is possible. Wide or negative-reaching intervals should not be mistaken for precise forecasts.

The method is based on [split-conformal residual calibration](https://arxiv.org/abs/2107.07511). Its usual exchangeability assumptions do not automatically hold for seasonally shifting, correlated football data with outcome-observation selection. Reported coverage is empirical and marginal within the graded category population—not a 90% guarantee for each player, each subgroup, or all market candidates.

## Abstention rules

Research eligibility is decided without test outcomes:

- Receiving is held out because of its zero-label sensitivity.
- Abstain if projected workload falls outside the calibration sample's 1st–99th percentile range.
- Abstain if the 90% interval width exceeds the calibration sample's 90th percentile of such widths.

These are initial, calibration-derived screening heuristics, not proof that the retained forecasts are safe. Coverage after filtering is separately reported; filtering does not inherit a conformal guarantee. In the scaled 2025 runs, 1,413 of 1,534 passing candidates and 4,449 of 4,929 rushing candidates pass the research screen. Their graded-subset 90% coverage is 88.34% and 90.52%, respectively.

**Every candidate abstains from market recommendations.** Availability before cutoff, sportsbook player identity, grading rules, and market-ready distribution validation are still unverified. The interval table enforces `market_abstain=true`. The postgame evidence audit and inferred sensitivity-only zeros are not used as calibration labels or pregame availability evidence.

## Saved artifacts and checks

Neon run IDs 27–50 represent 24 combinations of two evaluation seasons, three categories, two methods, and two nominal levels. The table contains 126,964 interval rows, representing four interval variants for each of 31,741 candidate player/game/category cases. There are 22,765 unique observed outcomes and 8,976 ungraded cases; repeated interval rows are not independent observations.

Each run saves the frozen model parameters, calibration quantiles, screening thresholds, source run IDs, empirical metrics, intervals, and abstention reasons. Original POU predictions remain unchanged. Thirty-one automated tests pass, including order-statistic boundaries, nested levels, missing-outcome exclusion, temporal separation, and rejection of postgame availability evidence.

## Next work

Improve and validate participation/zero-stat labels before full-market calibration. Preserve 2026 as new evaluation data and avoid tuning repeatedly against the already inspected 2023–2025 seasons. Quote ingestion and exact player/market matching can be developed behind the abstention gate, but these intervals alone cannot provide a calibrated probability of beating an arbitrary sportsbook line.

## Reproduce

```sh
python -m cfb.cli migrate
python -m cfb.cli interval-backtest --run-ids 17 18 19 20 21 22 23 24 25
python -m unittest discover -s tests -v
```

Consolidated report: `outputs/interval_comparison_50/summary.json`. Per-run reports and CSVs: `outputs/run_27/` through `outputs/run_50/`. Reruns create new IDs.
