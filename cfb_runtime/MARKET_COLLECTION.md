# Live prop collection — research only

From the project directory, independently of the running HTTP server:

```sh
.venv/bin/python -m cfb.cli collect-props --limit 5 --analyze-limit 3
```

Run `migrate` once before first use. This is a manual, bounded collection command,
not a scheduled job. It queries upcoming events in the next seven days and checks
the earliest five for US passing, rushing, and receiving yardage props. Defaults
allow at most 16 HTTP attempts (including retries), not 16 quota credits: provider
credits depend on markets returned. No historical-data purchase is made. Set
`--analyze-limit 0` to capture without inference. Event and analysis limits cannot
exceed ten. Empty coverage is recorded honestly; no synthetic lines replace it.

To broaden the sample across games without repeating existing combinations:

```sh
.venv/bin/python -m cfb.cli collect-props --limit 10 --analyze-limit 10 --new-only --per-game-limit 1
```

`--new-only` excludes game/player/market/line combinations already saved, including
responses that abstained for insufficient history. Quotes are still captured.
Changed lines may yield another correlated sample for the same player; this is
not a count of independent trials. `--per-game-limit` caps inference attempts,
including errors and null projections, for each event. Sampling follows provider
order within each game; it is not selected for favorable model probabilities.
These controls do not reserve work across concurrent collector processes; run
only one collector at a time when avoiding duplicates matters.

The Odds API documents player props at its per-event odds endpoint:
https://the-odds-api.com/sports/ncaaf-odds.html

New append-only application records are confined to `cfb_model_v1`:

- `prop_captures`: raw provider response, event identity, retrieval time, kickoff,
  suggested CFBD game, including events with no quotes.
- `prop_quotes`: bookmaker, market, exact line, side, American price, provider
  update time, candidate player/team IDs, and freshness/matching status.
- `forward_research`: linked quote, exact request and returned model analysis,
  saved before kickoff. It can include an abstaining/null projection. Outcome
  settlement and sportsbook grading rules are not implemented in this step.

Game suggestions require home/away orientation, school-name equality or a mascot
suffix, and kickoff within 15 minutes. They are **not verified mappings**. Player
suggestions require one normalized full-name identity across the two teams' prior
completed, same-season box scores before the current UTC day. No suffix removal,
fuzzy matches, transfer assumptions, or inferred injury clearance. Ambiguous or
missing identities remain unresolved. A prior appearance is not a current roster
or availability confirmation.

Only quotes updated within 15 minutes, not future-dated, can trigger research
inference. At most one inference per game/player/market/line per invocation;
repeated invocations deliberately retain new snapshots. Analysis errors do not
erase previously committed quotes. Captured live odds do not make historical
features point-in-time-vintaged: CFBD corrections and subsequent model changes
still need separate provenance controls before production validation.

All results retain `market_ready=false`, abstention and unverified identity flags.
No automatic bets, confidence grades, injury feed, ROI claims, or production
backend integration are enabled. Next: review actual coverage/identity candidates,
confirm availability and book-specific settlement, then append observed outcomes
without overwriting pregame predictions.

## First live check — October 1, 2026

Six HTTP calls checked five games. Four returned supported markets: 150 side-level
quotes total (not 150 distinct players or two-sided markets). All five events had
unverified game candidates; 147 quotes had unverified player candidates. Three
quotes for TJ Washington Jr. remained unresolved and were not analyzed.

Three research responses were saved and verified to precede provider kickoff:
Rodney Tisdale Jr. passing and CC Ezirim receiving abstained without projections
because history was insufficient; Jonathan Stafford Jr. receiving returned a
24.37-yard conditional projection against a captured 49.5 line. That receiving
result remains experimental and is not an endorsed wager. All three abstained
from recommendations. This is one numeric forward projection, not three scored
predictions or evidence of profitability. Tests: 39 passed.

## Results tracking

```sh
.venv/bin/python -m cfb.cli track-results
```

This evaluates saved research responses against the **currently ingested** CFBD
schedule and player box scores. It does not fetch new stats, schedule a recurring
job, or alter the saved pregame analysis. After games finish, refresh the schedule
with `games --years 2026` and player history with `player-history --years 2026`,
then run `track-results`. Those existing ingestion commands use the normal API
cache and bounded request budget; provider corrections may arrive later.

`cfb_model_v1.forward_outcomes` retains an observation whenever the evidence
changes. Consecutive unchanged runs add no rows. Corrections, including a return
to an earlier stat value, produce a new observation without overwriting history.
The report recalculates from current evidence; do not average all historical
observation rows, which would double-count revised outcomes.

Statuses distinguish pending games, missing box-score outcomes, identity/team
mismatches, late predictions, observed outcomes without projections, and observed
outcomes with an unverified candidate identity. Explicit zeros and negative yards
are valid observations; an absent row is neither zero nor a confirmed DNP/void.

The only metrics are candidate-identity projection MAE and a three-class
over/under/push Brier score (sum of three squared probability errors; not directly
comparable to earlier binary Brier scores). Invalid probability distributions
are excluded from Brier scoring. Repeated snapshots are correlated and not
independent trials. No verified betting win rate, P&L, or ROI is computed.
Identity review, injury/availability confirmation, and book-specific grading
rules remain unresolved; `market_ready` stays false.

## Roster identity evidence

```sh
.venv/bin/python -m cfb.cli audit-identities --limit 7
```

Checks the latest capture of up to seven upcoming games with quotes. Fetches fresh
CFBD FBS team metadata and both teams' season rosters (normally 15 successful
requests for seven games in one season). Full school-and-mascot names, orientation,
and kickoff within 15 minutes must agree before matching players. Full player
names are normalized for punctuation but not suffixes; duplicate identities
across either roster, absent names, and conflicts with prior box-score IDs remain
unresolved. New roster candidates are recorded, not automatically substituted.

`identity_sources` stores timestamped provider responses. `quote_identity_audits`
stores per-quote outcomes linked to those sources. Reruns intentionally append
new evidence; they do not rewrite captured quotes, pregame analyses, or tracking
grades. This audit does not retroactively assert that roster evidence was known
at the prediction cutoff. CFBD's roster `year` field is a player class/year, not
the requested season; the source request records the season separately.

`roster_corroborated` means the candidate agrees with current CFBD roster/name
data. It is **not** a verified sportsbook player-ID crosswalk, current injury
clearance, active-game designation, or assurance of participation. Availability
stays unknown and all betting recommendation gates remain closed. The audit is
currently an independent review command; the running HTTP endpoint is unchanged.

Provider schema reference: https://github.com/CFBD/cfbd-python/blob/main/docs/RosterPlayer.md

First audit (October 1, 2026): 15 HTTP calls covered seven games and 218 quote
rows. 209 prior candidate quote matches were corroborated. Seven quotes produced
new roster candidates for three players: Johnathan Montague Jr., Josh Derry, and
TJ Washington Jr. Two quotes for Randy Pittman Jr. remained name-unresolved.
These counts are quote rows, not unique players. No candidate substitutions,
injury assertions, or changes to frozen analyses were made. All 50 tests passed.
