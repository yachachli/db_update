# CFB POU output contract

The HTTP `/cfb_pou` endpoint and `analyze` CLI return NFL-style public fields:
`over_under`, `grade`, `league`, `injury`, `insights`, `input`,
`short_answer`, `long_answer`, `player_position`, `graphs`,
`projected_stat`, `projected_value`, `pre_injury_projected_value`,
`injury_adjustment_notes`, and `version`. League is CFB.

No research/betting-warning prose or diagnostic fields appear in this public
response. The side is the existing model lean. The renderer does not calculate
a new grade: the current internal grade remains 0. Null projections stay null;
missing history gets a plain data-availability message, not a fabricated pick.
Unknown injury status is not rewritten into a claim that the player is healthy.

## Participation

The field list above is unchanged. `analyze_internal` additionally returns
`participation_probability` and `unconditional_projected_value`; neither appears
as a public key, because adding one would be a shared-backend contract change.

Participation reaches the public response through the existing text fields. Every
supported projection gains an insight naming the probability that the player
records the stat at all, and the unconditional projection. Below 0.5 the renderer
withdraws the side (`over_under` becomes null) and `short_answer` leads with the
participation estimate rather than a confident-looking yardage number. Projecting
yardage for a player who probably records nothing is the misleading case this
prevents.

`participation_probability` is the probability that a box score records this
category. It is not confirmed availability, not an injury model, and not a
settlement rule. `injury` stays null and unknown is never rewritten as healthy.

`analyze_internal` retains the prior diagnostic result. Market collection calls
that internal function so frozen research responses, probabilities, outcome
tracking, and validation flags remain unchanged. Rendering does not alter
training, probabilities, cron configuration, or saved historical analyses.

The request still requires CFB game_id/player_id/stat/line; optional as_of is used
for replay. Public input mirrors the NFL fields and omits internal lookup fields.
The shared backend's league enum/routing must accept CFB before integration;
this change does not deploy or modify bestbet_backend or the NFL model.

Restart an already-running local CFB server to load the new renderer.
