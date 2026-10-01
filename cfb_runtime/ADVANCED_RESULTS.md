# Advanced feature comparison

Completed September 30, 2026. Neon run IDs 11–16 contain all six model/season evaluations, with 4,796 predictions and 4,796 input-feature records. The advanced warehouse contains 9,084 team-game rows for 2021–2025, each retaining paired filtered/full responses.

## Decision

Retain `score-ridge-v0` as the benchmark. The fixed `advanced-ridge-v2` specification does not improve pooled margin or total error. This does not prove the underlying features are useless; it means this particular representation and linear model have not justified promotion. No betting edge is established.

Pooled over the same 2,398 games from 2023–2025:

| Model | Margin MAE | Total MAE | Winner accuracy |
| --- | ---: | ---: | ---: |
| Score-only baseline | 13.346 | 12.968 | 69.60% |
| Baseline + advanced ratings | 13.368 | 13.073 | 69.56% |

The advanced model's margin MAE by year was 13.259, 13.686, and 13.159; baseline values were 13.345, 13.640, and 13.055. The small 2023 gain did not persist in 2024 or 2025. No significance test or parameter tuning was performed on these evaluation results.

## What was added

Four opponent-adjusted efficiency ratings—pass/rush success rate and pass/rush explosiveness—plus opponent-adjusted full-game play volume. Each has offensive and defensive-allowance components for each team, adding 20 features to the scoring baseline. Efficiency excludes provider-defined garbage time; volume includes it. Split-metric exposure uses total clean plays as a proxy because split denominators are unavailable in this response.

Historical ratings use only earlier UTC dates and up to two previous seasons, with fixed regularization and decay. Final feature scaling and margin/total regressions use prior-season rows only. The 2026 season is excluded. These are repeatedly inspected development folds, not untouched final validation. Historical provider revisions and uncertain publication timestamps remain limitations.

## Coverage exception

Four FBS-versus-FBS games lack advanced rows in 2024:

- 401644780: Western Michigan–Eastern Michigan
- 401644689: Miami (OH)–Kent State
- 401645328: Army–Rice
- 401641034: Sam Houston–New Mexico State

A targeted Army request also omitted the Army–Rice game. The exception is explicitly recorded in every run's configuration. Missing observations are excluded only from efficiency fitting; all scoring targets remain in both models' evaluation. No unavailable performance was replaced with zero. Coverage overrides are bounded to 1% of games and must be explicitly enabled. All 7,880 available FBS-versus-FBS team rows had complete metric values.

Twelve automated tests pass, including future/same-day leakage checks, filtered/full field separation, data validation, and the bounded coverage override.

## POU readiness and next implementation

The existing one-week player sample has passing attempts/yards, carries/rushing yards, and receptions/receiving yards. It does not establish complete participation or supply receiving targets. The next POU build should:

1. Load and audit multi-season player history, retaining stable player IDs and team changes.
2. Build prior-game volume and efficiency features without treating absent category rows as zero performances. Keep participation/availability assumptions explicit.
3. Compare chronological point-projection baselines for passing, rushing, and receiving yards before fitting probability distributions.
4. Add market-line timestamps, identity matching, and historical price coverage before any over/under accuracy or ROI claim.

No POU model was trained in this step.

## Reproduce

```sh
python -m cfb.cli migrate
python -m cfb.cli advanced-stats --years 2021 2022 2023 2024 2025
python -m cfb.cli advanced-backtest --years 2023 2024 2025 --allow-missing-games
python -m unittest discover -s tests -v
```

Reports: `outputs/advanced_comparison_16/` and `outputs/run_11/` through `outputs/run_16/`. Reruns receive new IDs.

Source contract: [CFBD advanced game-stat documentation](https://api.collegefootballdata.com/api/stats).
