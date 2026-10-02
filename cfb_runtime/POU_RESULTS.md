# POU model: cross-season eligibility and participation

Updated October 2, 2026. Neon runs 61-72 (`pou-volume-efficiency-v1`) and 73-78
(`cfb-pou-v2` release validation).

## Two changes

**History carries across seasons on the same team.** Eligibility previously
keyed history to `(season, team, category)`, which blacked out the opening weeks
of every season even for a returning starter with a full prior season on record.
A player who appeared in all of 2025 was still ineligible in weeks 1-3 of 2026.
History now keys to `(team, category)`, with a 400-day offseason gap limit and a
guard so a transferred player is never enumerated for their old team.

**Participation is modelled explicitly.** Roughly half of all candidates never
record a stat in their category. A conditional yardage projection on its own was
mis-specified for the population it was being applied to.

## Coverage

Candidate rows, 2023-2025:

| Category | Before | After | Change |
| --- | ---: | ---: | ---: |
| Passing | 4,638 | 9,869 | +113% |
| Rushing | 14,619 | 29,097 | +99% |
| Receiving | 28,127 | 62,643 | +123% |

Regular-season candidate rows by week, 2023-2025 pooled:

| Week | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before | 0 | 0 | 15 | 505 | 2,136 | 2,829 | 3,680 |
| After | 5,385 | 5,639 | 6,121 | 7,075 | 7,429 | 7,286 | 8,279 |

The extra coverage is not bought with accuracy. Scored on only the rows that
were *already* eligible, the carryover model is better in all three categories
(passing -0.342, receiving -0.291, rushing -0.048 MAE). On the newly unlocked
rows it beats the trailing-five baseline by 7.47 (passing), 2.38 (rushing) and
0.84 (receiving).

## Participation

Walk-forward, trained only on earlier seasons. The label is "a box score
recorded this category" - **not** confirmed availability, and not an injury model.

| Category | Candidates | Records a stat | AUC | Brier | vs base rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Passing | 9,869 | 46% | 0.925 | 0.095 | +60% skill |
| Rushing | 29,097 | 50% | 0.914 | 0.102 | +59% skill |
| Receiving | 62,643 | 41% | 0.883 | 0.124 | +46% skill |

Calibration is close across the full range; receiving deciles run
0.007/0.009, 0.013/0.014, 0.029/0.027 ... 0.908/0.907 predicted versus observed.

Scoring an absence as zero yards - one grading convention, not a settlement
rule - the unconditional projection roughly halves error:

| Category | Conditional only | Unconditional | Change |
| --- | ---: | ---: | ---: |
| Passing | 95.67 | 50.75 | -47% |
| Rushing | 26.98 | 16.60 | -38% |
| Receiving | 23.93 | 12.38 | -48% |

Thresholding on the probability gives a principled abstention rule in place of
the previous blanket abstain. At a 0.85 threshold, the share of retained
candidates that actually record a stat rises from roughly 40% to 90-93%.

## Release validation

Trained through 2024, calibrated on 2025, evaluated on 2026 through week 5:

| Category | Model MAE | Trailing-five MAE | Participation AUC |
| --- | ---: | ---: | ---: |
| Passing | 67.49 | 73.53 | 0.849 |
| Rushing | 25.31 | 28.72 | 0.870 |
| Receiving | 21.18 | 22.03 | 0.821 |

All three now beat the baseline on 2026. Under the previous model passing
(60.87 vs 57.65) and receiving (26.16 vs 24.00) both lost to it. The 2026 sample
is 244/830/1,344 graded rows and is not large enough for a strong claim, but it
was not used to select or tune anything.

## Tested and rejected

| Change | Result | Decision |
| --- | --- | --- |
| EWMA trailing window (14d/21d/35d) | +0.17 to +0.45 MAE, every category and half-life | Rejected |
| Teammate volume share and team volume | +0.006 to +0.063 MAE | Rejected |
| Context from the rebuilt game ratings | +0.076 / -0.006 / +0.025, inconsistent | Rejected for POU |
| Market-informed context (closing line as game script) | -0.206 / +0.006 / -0.024, noise-level | Rejected |

The rebuilt game ratings still ship, on the game model's own merit; they simply
do not move POU, whose features are dominated by a player's own recent history.

## Still outstanding

Participation is not availability. These projections remain research output:
identity is unverified against any sportsbook, grading rules are unconfirmed,
and no ROI is computed. `market_ready` stays false and the database still
enforces it.

## Reproduce

```sh
python -m cfb.cli pou-backtest --years 2023 2024 2025 2026 --allow-missing-games
python -m cfb.cli train-release
```
