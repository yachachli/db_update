# BestBet college football

This repository contains the first data and backtesting foundation for college-football game predictions and player over/under projections. PostgreSQL tables live exclusively in `cfb_model_v1` within the existing Neon database.

**Current runnable model:** [CFB POU v1](RELEASE_V1.md) provides a local `/cfb_pou` endpoint and CLI with projections, conditional line analysis, graphs, and abstention reasons. It is research-ready, not yet production betting-ready. Read the release report for the fixed-model backtests, mixed 2026 results, and data-source requirements.

## Setup

Use Python 3.12 for the pinned v1 runtime, create a virtual environment, and run `pip install -e . -c constraints-runtime.txt`. A dedicated `.venv` is provisioned in this workspace; activate it with `source .venv/bin/activate`. Credentials belong in the ignored `.env` (see `.env.example`). `DATABASE_URL` must be PostgreSQL. Never log the URL or API request URLs containing the odds key.

```sh
python -m cfb.cli migrate
python -m cfb.cli games --years 2021 2022 2023 2024 2025 2026
python -m cfb.cli players --year 2025 --week 1
python -m cfb.cli lines --years 2021 2022 2023 2024 2025 2026
python -m cfb.cli odds
python -m cfb.cli backtest --test-year 2025
python -m cfb.cli team-stats --years 2021 2022 2023 2024 2025
python -m cfb.cli efficiency-backtest --years 2023 2024 2025
python -m cfb.cli advanced-stats --years 2021 2022 2023 2024 2025
python -m cfb.cli advanced-backtest --years 2023 2024 2025
python -m cfb.cli player-history --years 2021 2022 2023 2024 2025
python -m cfb.cli pou-backtest --years 2023 2024 2025 --allow-missing-games
python -m unittest discover -s tests
```

The first build used a sibling project's environment. CFB POU v1 now has its own installed environment and pinned model-library versions; the sibling project is no longer a runtime dependency.

## Current baseline

`score-ridge-v0` fits offense and opposing defensive scoring allowance jointly, with home advantage and prior-season decay. Each day's features use earlier dates only. A second ridge model maps those ratings to final margin and total, trained on earlier seasons. Final scoring labels include overtime. Only completed FBS-versus-FBS games enter the baseline; FCS fixtures remain in the warehouse for later calibrated modeling. The model is a scoring-strength baseline, not yet the intended passing/rushing efficiency model.

Backtests save immutable run-specific predictions in Neon and local metrics/CSV files under ignored `outputs/`. Test-season scores update ratings only for subsequent dates, which simulates weekly operation. All transformations must retain that temporal discipline as richer features are added. Revised historical source data is not an archive of what the provider originally published.

## Next build stages

1. Audit advanced team statistics and player categories; expand ingestion with coverage reports and reconciliation tests.
2. Fit separate pass/rush offense and defense ratings, regularized for sample size and schedule connectivity. Retain continuous ratings and uncertainty; derive display brackets afterward.
3. Reconstruct preseason talent, returning production, transfers, and coaching context. Avoid season-end adjusted ratings as earlier-season features.
4. Compare fundamental and market-aware game models across rolling season folds, with calibrated probabilities and interval coverage. Keep one final season untouched during tuning.
5. Normalize player box scores and reconstruct opportunity shares. Train POU volume and efficiency models with player/team continuity, game-script scenarios, and calibrated over/under probabilities.
6. Map sportsbook event/player IDs explicitly, record available prop markets and quote timestamps, and distinguish no market from a valid zero. Add availability inputs before issuing production POU recommendations.
7. Add serving contracts compatible with BestBet, scheduled incremental updates, data-quality gates, and prediction grading.

No betting edge or production readiness is established by the initial score baseline. Historical prop ROI requires historical prop lines and prices; ordinary player box scores alone cannot validate it.

## Opponent-adjusted efficiency layer

See [RESULTS.md](RESULTS.md) for the verified 2023–2025 comparison and the decision to retain the score-only benchmark.

`team-stats` pulls regular and postseason team box scores in schedule-derived week partitions, validates game/team IDs, and upserts normalized values in Neon. Missing values remain null; negative rushing yards are valid. The backtest refuses incomplete game coverage rather than silently dropping games. Missing metric values also fail by default. After inspecting the data, `--allow-missing-stats` permits up to 1% incomplete team rows, records the missing game IDs in every run, and skips missing observations only in the relevant efficiency fits; all game targets remain in evaluation. The 2022 Buffalo–Akron game (401506450) lacks offensive totals in both weekly and game-specific CFBD responses, requiring this explicit override for the initial comparison.

`opponent-efficiency-v1` jointly estimates pass offense, pass defensive allowance, rush offense, and rush defensive allowance from game-level yards per attempt. A home-field term is included in each efficiency fit. Attempts weight observations (30 attempts = one unit); prior seasons receive weight `0.45 ** seasons_ago`, with a two-prior-season window. Ridge penalty 12 shrinks small samples toward average. Positive offense is better; **negative defensive allowance is better**. This is box-score efficiency, not EPA: overtime, sacks as recorded in college box scores, game script, and garbage time are not disentangled yet.

Each UTC day's ratings exclude all games on that date and later. Eight team ratings augment the score baseline; scaling and the final margin/total regressions are fit only on earlier seasons. Three rolling folds compare the original score baseline, unadjusted/shrunk box-score efficiency, and joint opponent-adjusted efficiency. Parameters are fixed before the comparison, not selected on each test season. The comparisons are development evidence, not untouched final validation or betting ROI.

Neon stores run-specific evaluation features and predictions. `outputs/efficiency_comparison_<id>/` contains the comparison and historical rating snapshots. Snapshots are dated before the final evaluation slate, **not current live rankings**. Brackets are percentile bands (top 10%, next 20%, middle 40%, next 20%, bottom 10%) derived separately for each rating, not inputs to training. Exposure below three weighted 30-attempt units is labeled insufficient sample. Exposure is a sample-support indicator, not a confidence interval; sparse schedule connections still need further treatment. FCS results are stored but excluded from these fits.

Endpoint contract: [CFBD game/team box-score documentation](https://api.collegefootballdata.com/api/games).

## Advanced game-level features

See [ADVANCED_RESULTS.md](ADVANCED_RESULTS.md) for the verified comparison, data exceptions, and POU readiness audit. The initial advanced model did not beat the score baseline overall.

`advanced-stats` stores paired garbage-time-filtered and full-game responses from [CFBD advanced game statistics](https://api.collegefootballdata.com/api/stats). IDs are resolved through each game's exact team/opponent names and validated before storage. The source includes games outside our schedule; those are counted and excluded explicitly.

`advanced-ridge-v2` adds opponent-adjusted pass/rush success rate, pass/rush explosiveness, and full-game offensive play volume to the score baseline. Split efficiency observations are weighted by clean total plays / 60, because this endpoint does not provide separate pass/rush or successful-play counts; this is an exposure proxy, not a true split sample count. Volume uses one weight unit per game. Every metric uses the same fixed ridge penalty and season decay as v1. Offense and opponent allowance are fitted jointly, with a home-field adjustment. All inputs exclude the prediction's UTC date. No season-end aggregate is used as an earlier-season feature.

The final mapping uses training-season-only standardization and a fixed ridge penalty. Validation repeats the same 2023–2025 game sets against the unchanged scoring baseline. No hyperparameter search uses these evaluation labels. Models remain development candidates until improvement is consistent and confirmed on genuinely new data. Advanced coverage gates require two team rows per game and finite, present metric values. An inspected missing-game exception can be logged with `--allow-missing-games` (maximum 1% of games); it omits unavailable advanced observations from rating fits but retains all scoring targets. Four 2024 games require this override in the initial dataset: 401644780, 401644689, 401645328, and 401641034. A targeted Army request also omitted game 401645328. Missing observations are not replaced with zero-valued performances.

The POU data includes `passing` (`C/ATT`, `YDS`), `rushing` (`CAR`, `YDS`), and `receiving` (`REC`, `YDS`). A player missing from a category must not automatically be labeled as a zero-yard performance or a DNP. Receptions are not targets; this endpoint does not provide a target-share denominator.

## Player projection baseline

The subsequent [interval evaluation](INTERVAL_RESULTS.md) adds frozen-model chronological 80%/90% research intervals and abstention rules. All market recommendations remain blocked. Run `python -m cfb.cli interval-backtest --run-ids 17 18 19 20 21 22 23 24 25` against the original POU runs.

See [POU_RESULTS.md](POU_RESULTS.md) for the verified results and conditional-participation limitations. The initial models beat trailing-five and last-game yardage baselines in every tested category/season, but are not yet market-ready probability models.

**Subsequent outcome audit:** [OUTCOME_AUDIT.md](OUTCOME_AUDIT.md) shows that receiving loses its baseline advantage under a plausible inferred-zero sensitivity scenario. Keep receiving experimental. The audit preserves original labels and separates explicit outcomes, inferred sensitivity-only zeros, and unresolved cases. Run `python -m cfb.cli outcome-audit --run-ids 17 18 19 20 21 22 23 24 25` to reproduce against the original POU runs.

`player-history` loads regular/postseason partitions from the completed schedule, validates team names against game IDs, and stores normalized offense in `cfb_model_v1.player_offense`. Missing volume/yards pairs are audited and not zero-filled. Full source responses remain in the credential-free local cache. The initial one-week raw-stat table is preserved.

`pou-volume-efficiency-v0` fits separate fixed ridge models for opportunity volume and yards per opportunity, with train-only standardization. Efficiency training is volume-weighted and uses only positive-volume observations. Forecast yards are predicted volume (floored at zero) multiplied by predicted efficiency; negative yardage outcomes are preserved. The features include trailing-five volume/yardage summaries, prior efficiency, time since last appearance, prior game count, opponent pass/rush allowance, and pregame score ratings. No actual same-day workload or performance enters a feature.

Candidates are enumerated **from earlier history**, requiring three prior same-team, same-season FBS-versus-FBS category appearances, an appearance within 45 days, and trailing-five average volume of at least 5 passing attempts, 3 carries, or 1 reception. History resets each season and does not transfer across teams. This intentionally excludes cold starts and early-season projections. An old-team candidate can remain after a transfer until the recency gate expires; without roster/availability information it must remain an unconfirmed candidate, not an active-player recommendation.

All candidates are saved, including those with unavailable next-game category outcomes. Metrics grade only observed category outcomes and are therefore **conditional participation metrics**, not an unbiased evaluation of all players or sportsbook props. Zero opportunities are scored only when explicitly present in the source. Absence can mean no activity, injury, transfer, or incomplete coverage and stays ungraded. The 2022 Buffalo–Akron game has no usable offensive player history and requires the bounded, logged `--allow-missing-games` override. No game is assigned fabricated player zeros.

The test compares 2023, 2024, and 2025 against trailing-five and last-game yardage baselines using identical graded rows. Each run saves candidate predictions, null outcomes, feature values, training configuration, and model parameters in Neon. Local reports and readable parameter snapshots are under `outputs/run_<id>/`. `predict_snapshot` reproduces projections from stored parameters without loading pickled code. This is a point-projection baseline—not calibrated over/under probabilities, a participation model, or a betting ROI evaluation.

## API reference

- https://api.collegefootballdata.com/api
- https://api.collegefootballdata.com/data-availability
- https://the-odds-api.com/liveapi/guides/v4/

The HTTP client uses bounded requests, three attempts, a per-run call budget, and credential-free cache metadata. Current and previous calendar-year pulls expire after one hour; older seasons are cached until manually invalidated. Detailed quota-cost budgets are follow-up work.

## First verified run

The initial load stored 5,435 games across 2021–2026 (including scheduled games), player box scores for 96 games from 2025 week 1, and 58 current game-odds snapshots. These counts describe ingestion coverage, not usable training samples for every market.

The 2025 chronological baseline trained its final mapping on 2,997 prior-season feature rows and evaluated 808 FBS-versus-FBS games. Winner accuracy was 71.29%; home-margin MAE 13.05 points versus 15.89 for the constant baseline; total MAE 12.81 versus 13.38. These are exploratory baseline results, not an untouched final test or a sportsbook comparison. No player-prop model has been trained yet, and no prop ROI has been measured. Keep future evaluation seasons separate from development decisions.

Local reports are stored in `outputs/run_<id>/`; Neon stores each run and its predictions under the same run ID. The temporal unit test verifies that changing same-day/future results cannot alter earlier features. The cutoff uses UTC date rather than verified publication times, so this is a reconstructed historical backtest, not a point-in-time data archive.

## Repository ownership

This repository owns POU serving, frozen POU artifacts (private release assets),
and the shared ingestion runtime. The separate cfb-game-predictions repository
owns game-model research. Shared rating modules are versioned source snapshots,
not independently scheduled ingestors. Install the repositories in separate
environments because both expose the `cfb` Python package.
