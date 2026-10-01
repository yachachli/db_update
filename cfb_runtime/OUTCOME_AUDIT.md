# Participation and zero-yardage audit

Completed September 30, 2026. Neon audit run 26 classifies all 47,384 candidate predictions from POU runs 17–25. Original predictions, verified outcomes, and training labels were not changed.

## Main finding

The receiving model's small advantage is **not robust to plausible missing zero-yard outcomes**. When activity-backed, team-reconciled inferred zeros are included in a separate sensitivity calculation, the model's receiving-yard MAE becomes 22.15 versus 21.70 for the trailing-five baseline. Lower is better. Keep receiving projections experimental; do not translate their earlier conditional improvement into a claim of sportsbook edge.

Passing and rushing retain an advantage in this sensitivity scenario, but the unresolved population remains too large to establish market-ready accuracy or profitability.

| Category | Explicit-outcome model / baseline MAE | Sensitivity model / baseline MAE | Inferred zeros |
| --- | ---: | ---: | ---: |
| Passing | 68.29 / 70.79 | 69.25 / 71.03 | 78 |
| Rushing | 26.71 / 27.55 | 26.57 / 27.33 | 290 |
| Receiving | 22.10 / 22.40 | 22.15 / 21.70 | 2,592 |

The sensitivity populations differ from the explicit-outcome populations. These are assumption checks, not corrected ground-truth metrics. No models were retrained and no formal significance testing was performed.

## Outcome accounting

- 33,771 outcomes have explicit source yardage, including 355 explicit zero-yard rows. These zeros were already graded in the original backtests, not newly recovered.
- 2,960 missing outcomes have recorded player activity plus exact category-level reconciliation of reported player volume **and** yards against team totals. They receive a sensitivity-only zero, never a verified zero.
- 19 missing outcomes have positive activity evidence but do not pass the reconciliation rule.
- 10,634 missing outcomes have no positive evidence in the inspected cache. This does **not** establish that the player did not participate.
- Consequently, all 13,613 originally ungraded outcomes remain unverified; 10,653 also lack the narrower sensitivity-only inference.

Evidence comes primarily from the already-cached full historical player box scores. Positive counts—not mere roster/list presence, zero-valued stat rows, or efficiency percentages—establish activity evidence. A targeted play-stat check of game 401752841 found three Target events for athlete 4879567 despite no receiving category row. That sample supports the need for better zero-catch handling; it is not a comprehensive play-by-play audit.

Matching team totals does not establish individual participation/settlement rules or prove that the source is complete. Yardage can include unusual scoring/lateral treatment, and provider omissions or revisions remain possible. The inference is deliberately not promoted to training truth.

## Implemented safeguards

`player_outcome_audits` stores explicit and sensitivity-only labels in separate columns, with source run IDs, cached evidence provenance, reconciliation flags, and postgame-only designation. No source prediction or model feature is overwritten. Postgame evidence must not become a same-game pregame feature.

A tested `recommendation_gate` helper rejects unknown availability, availability evidence later than the prediction cutoff, invalid timestamps, unverified identities, unvalidated distributions, and unknown grading rules. This is a building block for a future serving path, not a deployed betting service or proof that the necessary feeds are connected. Its input confirmations must ultimately be tied to the correct player/game and reliable provenance.

Twenty-five automated tests pass, including explicit-zero versus inferred-zero separation, exact volume/yard reconciliation, positive activity checks, and rejection of postgame availability evidence.

## What is still needed

Reliable participation and zero-stat labels remain unresolved. Missing records must not be filled as zeros wholesale. We can next evaluate **conditional** prediction intervals with clear abstention rules, but full-market probability calibration requires a defensible participation/zero-catch process and validation on newly held-out data. An authoritative participation/snap feed, complete reconciled play-level data, or reviewed official gamebooks may be needed; none is currently integrated. No paid data service has been purchased or added.

## Reproduce

```sh
python -m cfb.cli migrate
python -m cfb.cli outcome-audit --run-ids 17 18 19 20 21 22 23 24 25
python -m unittest discover -s tests -v
```

Reports are in `outputs/outcome_audit_26/`; a rerun receives a new audit ID. The audit uses local cached snapshots plus warehouse values, so retain the credential-free raw cache for reproducibility.

CFBD's [play-stat documentation](https://api.collegefootballdata.com/api/plays) describes the association endpoint and its 2,000-record response limit. Positive events can provide evidence; absence from a capped or incomplete response cannot prove nonparticipation.
