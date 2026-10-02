# Game model: market benchmark and rating rebuild

Updated October 2, 2026. Supersedes the September 30 opponent-adjusted efficiency
comparison, whose conclusion (do not promote either efficiency variant) still stands.

## The headline finding

The game model carries **no information the closing line has not already priced.**
Regressing actual margin on both the consensus closing line and the model's own
forecast, over the same 2,398 FBS-versus-FBS games from 2023-2025:

| term | coefficient | std. error | t |
| --- | ---: | ---: | ---: |
| market margin | 0.9906 | 0.0428 | 23.12 |
| model margin | 0.0346 | 0.0492 | 0.70 |

The market coefficient is indistinguishable from one; the model coefficient is
indistinguishable from zero. Blending the model into the line changes
out-of-sample error by +0.001 points, which is nothing.

This was re-tested after the rating rebuild below, which cut model margin error
by more than half a point. The model coefficient **fell** to -0.0221 (t = -0.31).
Making the model more accurate did not make it more informative about the line.

**Therefore: no betting edge is demonstrated, and none should be claimed.** More
features of the same kind - opponent-adjusted season aggregates built from box
scores - are not a promising route to one. A future edge claim has to be measured
as error against the closing line, not as raw MAE, and this file is where that
measurement belongs.

## Rating rebuild

Two changes, both validated on held-out 2026 data that was used for no tuning:

1. **Separate regularization for margin and total.** Margin is a difference of
   four team coefficients, so shrinkage pulls every matchup toward a coin flip.
   Total is a sum, which benefits from much heavier shrinkage. One shared alpha
   was a compromise that suited neither. Margin now fits at alpha 0.2, total at
   alpha 20. Both are interior optima, not grid edges.
2. **Recency decays on days, not seasons.** The previous `0.45 ** (year - season)`
   step weighted a week-1 game exactly like a week-13 game. Replaced with a
   240-day half-life. Lookback widened from 2 seasons to 3.

Pooled over 2,398 games, 2023-2025 (chronological development folds):

| Model | Margin MAE | Total MAE | Winner accuracy |
| --- | ---: | ---: | ---: |
| Previous `score-ridge-v0` | 13.345 | 12.967 | 69.64% |
| Current `score-ridge-v1` | 12.804 | 12.919 | 70.73% |
| Consensus closing line | 12.000 | 12.685 | 72.49% |

Paired bootstrap over 4,000 resamples: margin -0.541, 95% CI [-0.682, -0.393];
total -0.048, 95% CI [-0.082, -0.011]. Margin improves in all three seasons
independently (-0.519, -0.558, -0.546).

### Held-out 2026

The 2026 season through week 5 (217 completed FBS-versus-FBS games) was used for
no tuning of any kind:

| Model | Margin MAE | Total MAE | Winner accuracy |
| --- | ---: | ---: | ---: |
| Previous `score-ridge-v0` | 14.370 | 12.184 | 73.73% |
| Current `score-ridge-v1` | 12.566 | 12.167 | 80.18% |
| Consensus closing line | 11.024 | - | 83.87% |

The gain is larger out of sample than in development. 217 games is a small
sample and the winner-accuracy figures in particular should not be read as a
stable estimate, but the direction is consistent with the development folds.

## Tested and rejected

Each of these was implemented, measured on identical evaluation games, and
reverted. They are recorded so they are not retried without new evidence.

| Change | Result | Decision |
| --- | --- | --- |
| Rest days / bye-week differential | margin 13.303 to 13.303; both together 13.322 | Rejected, no effect |
| Replacing the two-stage map with the raw rating | margin 13.833 vs 13.303 | Rejected, the second stage is worth 0.530 |
| Day half-life 120d | margin +0.122, inconsistent across seasons | Rejected |
| Day half-life 180d | margin -0.048 but sign-flips in 2025 | Rejected for 240d, which is consistent |
| Closing line as a model feature | margin 12.017, identical to the line alone | Not adopted; see below |

The line as a feature produces a better predictor but the fitted coefficient on
the ratings is zero, so it is the line wearing a model's clothes. It is recorded
in `market_benchmark` on every backtest run rather than smuggled into the
feature set.

## Betting lines

`cfb_model_v1.betting_lines` holds 15,411 provider quotes for 2021-2026, ingested
through `python -m cfb.cli lines --years ...`. Coverage on the evaluation set is
100% of games, median 3 books per game. `backtest.run` now records a
`market_benchmark` block on every run so the comparison can never quietly lapse.

## Reproduce

```sh
python -m cfb.cli migrate
python -m cfb.cli lines --years 2021 2022 2023 2024 2025 2026
python -m cfb.cli backtest --test-year 2025
python -m unittest discover -s tests
```
