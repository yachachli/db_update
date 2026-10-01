# Opponent-adjusted efficiency: first comparison

Completed September 30, 2026. All nine evaluation runs and their input features are saved in Neon `cfb_model_v1` (run IDs 2–10).

## Outcome

The new ratings are implemented, but the expanded model does **not** outperform the score-only baseline overall. Keep `score-ridge-v0` as the benchmark; do not promote either efficiency variant on this evidence.

Results pooled over 2,398 distinct evaluation games across 2023–2025 (errors in points; lower is better):

| Model | Margin MAE | Total MAE | Winner accuracy |
| --- | ---: | ---: | ---: |
| Score-only baseline | 13.346 | 12.968 | 69.60% |
| Baseline + unadjusted efficiency | 13.388 | 13.015 | 69.39% |
| Baseline + opponent-adjusted efficiency | 13.372 | 13.018 | 69.39% |

Opponent-adjusted margin MAE by season: 13.171 (2023), 13.748 (2024), 13.197 (2025). Score-only comparison: 13.345, 13.640, 13.055. The gains are not consistent across seasons, and no significance or betting-edge claim is made. These are chronological development folds: later folds train on earlier evaluation seasons once those seasons are in the past.

## Delivered

- 9,092 team-game box scores stored for 2021–2025, including FCS matchups retained for future work.
- Separate continuous passing/rushing offense and defensive-allowance ratings, jointly opponent-adjusted, attempt-weighted, and shrunk toward average.
- Historical percentile brackets with low-exposure labels, derived for display rather than used as model inputs.
- Identical evaluation-game sets for all models; train-only feature scaling for the efficiency models.
- 7,194 prediction rows and 7,194 associated feature records across nine runs.
- Eight passing automated tests covering temporal leakage, rating direction, missing values, zero attempts, parsing, caching, and API call budgets.

## Data exception and limitations

Both team rows for 2022 Buffalo–Akron (game 401506450) lack offensive totals in CFBD's weekly and game-specific responses. An explicit, logged override excludes those observations from efficiency fitting without excluding the game's scoring target. Two of 7,888 FBS-versus-FBS team rows are affected. Missing values were not replaced with zero.

Box-score efficiency includes game-script and overtime effects and does not isolate sacks, garbage time, success rate, or explosiveness. Defensive allowance below zero indicates better defense. Sample exposure is not a statistical confidence interval. The historical rating export is dated before the last evaluation slate, not a live 2026 ranking. The 2026 season was not used in this comparison.

## Next development step

Add game-level success rate, explosiveness, and play-volume features with the same chronological checks; test incremental value against the unchanged baseline. Separately expand player-stat history and build a volume/efficiency POU baseline. This work has not yet trained the player-prop model, calibrated over/under probabilities, or measured sportsbook ROI. Keep future evaluation data separate from development decisions.

## Reproduce and inspect

```sh
python -m cfb.cli migrate
python -m cfb.cli team-stats --years 2021 2022 2023 2024 2025
python -m cfb.cli efficiency-backtest --years 2023 2024 2025 --allow-missing-stats
python -m unittest discover -s tests -v
```

Local run reports are in `outputs/run_2/` through `outputs/run_10/`. The consolidated comparison, quality exception, and historical ratings are in `outputs/efficiency_comparison_10/`. Each rerun creates new IDs.
