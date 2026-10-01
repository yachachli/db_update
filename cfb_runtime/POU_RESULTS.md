# First player-yardage models

Completed September 30, 2026. `pou-volume-efficiency-v0` is a reproducible, conditional point-projection baseline for passing, rushing, and receiving yards. It is not yet a sportsbook-ready over/under probability model.

## Outcome

**Later audit:** See [OUTCOME_AUDIT.md](OUTCOME_AUDIT.md). Receiving's conditional advantage reverses when activity-backed, team-reconciled inferred zeros are included in a separate sensitivity check. The initial results below remain accurate for their explicit-outcome population, but are not robust evidence of performance on all prop candidates.

The two-stage volume/efficiency models reduced mean absolute yardage error versus both trailing-five and last-game yardage baselines in all nine category/season comparisons. Gains over trailing-five averages are modest and have not undergone statistical significance testing.

Pooled 2023–2025 errors, calculated on identical graded rows within each category (yards; lower is better):

| Category | Model MAE | Trailing-five MAE | Last-game MAE |
| --- | ---: | ---: | ---: |
| Passing | 68.29 | 70.79 | 85.06 |
| Rushing | 26.71 | 27.55 | 33.43 |
| Receiving | 22.10 | 22.40 | 27.83 |

2025-only model / trailing-five MAE: passing 68.62 / 71.06; rushing 26.60 / 27.53; receiving 22.00 / 22.29. No parameters were tuned using these results. The 2026 season was not used. These are development backtests, not independent final validation or measured betting ROI.

## Persisted and verified

- 131,440 normalized offensive player-category rows from 2021–2025, with 111,808 belonging to FBS-versus-FBS games used in this pipeline.
- Nine chronological model runs, Neon IDs 17–25. Each test year trains only on earlier seasons. The 2025 fold's models therefore train through 2024, not through 2025.
- 47,384 pre-result candidate projections: 33,771 graded and 13,613 ungraded.
- Feature values, projected opportunity and yards, trailing-five baseline, nullable observed outcomes, and model parameters stored in `cfb_model_v1.player_predictions` / `prediction_runs`.
- Readable scaling/coefficient snapshots saved locally; round-trip tests reproduce the fitted model's predictions.
- Twenty passing automated tests, including candidate eligibility without future player knowledge, same-day/future leakage, missing outcomes, season/team resets, stale history, parser checks, and saved-model reproducibility.

## Scope matters

Candidates require three prior same-team, same-season **FBS-versus-FBS category appearances**, with the last appearance within 45 days. Trailing-five average volume must be at least five passing attempts, three carries, or one reception. Features and candidates are created before the prediction date's outcomes are joined. Histories are not carried across teams or seasons. Early-season players, new roles, freshmen, and transfers without sufficient new-team history are consequently outside this first model's coverage.

The model forecasts volume and efficiency separately. Receiving volume means receptions, **not targets**. Pregame context uses prior-date score ratings and opponent pass/rush allowance. Training is standardized using earlier-season rows only; efficiency regression is weighted by observed positive volume.

Missing category rows are **not** zero yards and are **not** declared DNPs. A missing receiving row might mean zero catches, injury, changed usage, incomplete reporting, or another cause. The 13,613 ungraded candidates must not be ignored when assessing production usefulness: the reported accuracy is conditional on an observed category entry and may not carry over to real sportsbook populations. Confirmed zero-stat handling and availability are prerequisites for trustworthy market probabilities.

The 2022 Buffalo–Akron game (401506450) lacks usable offensive player records for both teams. This is explicitly logged under the bounded `--allow-missing-games` override. It contributes no fabricated player performance. All other FBS-versus-FBS game/team pairs had some offensive player observations; this does not prove every individual participant was reported.

Raw source data is retrospective and may include provider revisions. UTC date is the cutoff proxy, not a verified provider-publication timestamp. Results do not establish calibrated uncertainty, over/under accuracy, profitability, or readiness for automatic recommendations. No live 2026 forecasts were issued.

## Next step

Resolve participation and zero-stat outcomes as far as the available data permits, and measure remaining coverage gaps. Then develop and validate yardage distributions / interval coverage on chronological splits. Only after that should sportsbook player identity, line timestamps, over/under probabilities, and price comparisons be connected. Retain explicit abstention when a player's availability or grading basis is unknown.

## Reproduce

```sh
python -m cfb.cli migrate
python -m cfb.cli player-history --years 2021 2022 2023 2024 2025
python -m cfb.cli pou-backtest --years 2023 2024 2025 --allow-missing-games
python -m unittest discover -s tests -v
```

Consolidated comparison: `outputs/pou_comparison_25/`. Per-run CSVs, configuration, metrics, and parameter snapshots: `outputs/run_17/` through `outputs/run_25/`. `outputs/player_ingestion_audit.json` records returned-game coverage and incomplete player metric pairs; the backtest quality report separately records games that returned no usable offensive players. Future reruns receive new IDs.
