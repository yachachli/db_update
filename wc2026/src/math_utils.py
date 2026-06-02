"""Mathematical helpers for the World Cup 2026 predictive model.

Houses the core probability machinery: Poisson goal-expectancy, the
score-line probability matrix, the Dixon-Coles low-score correction
factor, and aggregation of the matrix into win/draw/loss probabilities.

It also holds the match-weighting primitives (opponent strength,
venue multipliers, match-quality weights, weighted averaging) and the
tournament-baseline computation that downstream rating normalization
depends on.
"""

from __future__ import annotations

import logging

import numpy as np
from scipy.stats import poisson

from src.config import (
    BASELINE_GOALS_FLOOR,
    BASELINE_MIN_MATCH_COUNT,
    DIXON_COLES_TAU,
    FIFA_POINTS_FILTER_THRESHOLD,
    FIFA_STRENGTH_EXPONENT,
    MAX_GOALS_FOR_MATRIX,
    REFERENCE_FIFA_POINTS,
    VENUE_MULT_AWAY,
    VENUE_MULT_HOME,
    VENUE_MULT_NEUTRAL,
    XG_DAMPENING_ALPHA,
    XG_HARD_CEILING,
)
from src.models import MatchStats, Team, TournamentBaseline

logger = logging.getLogger(__name__)

__all__ = [
    "opponent_strength",
    "venue_multiplier",
    "match_quality_weight",
    "weighted_average",
    "compute_baseline_goals",
    "poisson_pmf",
    "dixon_coles_correction",
    "compute_scoreline_matrix",
    "matrix_to_probabilities",
    "dampen_xg",
]


# ---------------------------------------------------------------------------
# Match weighting
# ---------------------------------------------------------------------------
def opponent_strength(opponent_fifa_points: float | None) -> float:
    """Return a multiplier reflecting how strong an opponent was.

    A result against a strong opponent should count for more than the same
    result against a weak one. We scale the opponent's FIFA points against a
    reference (median qualifier) baseline and compress the ratio with an
    exponent (square-root by default) so the adjustment is meaningful but not
    runaway.

    Formula:
        ``(opponent_fifa_points / REFERENCE_FIFA_POINTS) ** FIFA_STRENGTH_EXPONENT``

    Missing FIFA points (``None``) are treated as a median opponent and yield
    a neutral multiplier of ``1.0``.
    """
    if opponent_fifa_points is None:
        return 1.0
    return (opponent_fifa_points / REFERENCE_FIFA_POINTS) ** FIFA_STRENGTH_EXPONENT


def venue_multiplier(venue: str) -> float:
    """Return the match-quality multiplier for a given venue.

    Away results reflect more strongly on a team's quality than home results,
    with neutral venues (as at the World Cup itself) as the baseline. Maps the
    venue string to the corresponding configured multiplier.

    Raises:
        ValueError: if ``venue`` is not one of ``home``, ``away``, ``neutral``.
    """
    multipliers = {
        "home": VENUE_MULT_HOME,
        "away": VENUE_MULT_AWAY,
        "neutral": VENUE_MULT_NEUTRAL,
    }
    try:
        return multipliers[venue]
    except KeyError:
        raise ValueError(
            f"Invalid venue {venue!r}; expected one of "
            f"{sorted(multipliers)}."
        ) from None


def match_quality_weight(opponent_fifa_points: float | None, venue: str) -> float:
    """Return the combined quality weight for a single match.

    Multiplies the opponent-strength multiplier by the venue multiplier, so a
    match's contribution to a team's rating reflects both *who* they played
    and *where* the match was played.
    """
    return opponent_strength(opponent_fifa_points) * venue_multiplier(venue)


def weighted_average(values: list[float], weights: list[float]) -> float:
    """Return the weighted average of ``values`` under ``weights``.

    Standard formula ``sum(v * w) / sum(w)``. If the weights sum to zero
    (e.g. every contributing match had zero weight), this falls back to the
    unweighted arithmetic mean so callers never divide by zero.

    Raises:
        ValueError: if ``values`` is empty or lengths differ.
    """
    if len(values) != len(weights):
        raise ValueError(
            f"values and weights must be the same length; got "
            f"{len(values)} and {len(weights)}."
        )
    if not values:
        raise ValueError("weighted_average requires at least one value.")

    values_arr = np.asarray(values, dtype=float)
    weights_arr = np.asarray(weights, dtype=float)
    weight_total = weights_arr.sum()
    if weight_total == 0:
        return float(values_arr.mean())
    return float((values_arr * weights_arr).sum() / weight_total)


# ---------------------------------------------------------------------------
# Tournament baseline
# ---------------------------------------------------------------------------
def compute_baseline_goals(
    all_matches: list[MatchStats], teams: dict[int, Team]
) -> TournamentBaseline:
    """Compute tournament-wide scoring baselines used to normalize ratings.

    Lopsided fixtures (a top side thrashing a minnow) distort the notion of a
    "normal" scoreline, so matches where the FIFA-points gap between the two
    teams exceeds ``FIFA_POINTS_FILTER_THRESHOLD`` are excluded. From the
    surviving pool we compute:

    - ``baseline_goals_per_match``: mean total goals (both teams) per match,
      where each ``MatchStats`` contributes ``goals_scored + goals_conceded``
      (i.e. the full scoreline from one team's perspective).
    - ``baseline_goals_per_team``: that figure divided by two, i.e. the
      expected goals for a single, average team.

    Teams missing from ``teams`` (no FIFA data) cannot have their gap
    evaluated and are skipped.

    Two further safeguards keep the baseline honest against data-quality gaps:

    - **Zero-stat exclusion.** Matches whose core stats (goals scored,
      goals conceded, xG created, xG conceded) are *all* zero are almost
      certainly missing-data fixtures (e.g. AFC qualifiers without xG
      coverage), not genuine goalless games, so they are dropped. Counting
      their zeros would otherwise deflate the baseline and inflate every
      normalized rating.
    - **Safety floor.** If the computed per-team baseline is implausibly low
      (``< 1.0``) or no matches survive filtering, it falls back to
      :data:`BASELINE_GOALS_FLOOR`. A thin surviving pool
      (``< BASELINE_MIN_MATCH_COUNT``) is logged as a low-confidence warning.
    """
    total_goals = 0.0
    filtered_match_count = 0
    excluded_zero_stat_matches = 0

    for match in all_matches:
        team = teams.get(match.team_id)
        opponent = teams.get(match.opponent_id)
        if team is None or opponent is None:
            continue
        if abs(team.fifa_points - opponent.fifa_points) > FIFA_POINTS_FILTER_THRESHOLD:
            continue
        # Drop matches with all-zero core stats: a missing-data signal, not a
        # real 0-0 with no shots.
        if (
            match.goals_scored == 0
            and match.goals_conceded == 0
            and match.xg_created == 0
            and match.xg_conceded == 0
        ):
            excluded_zero_stat_matches += 1
            continue
        total_goals += match.goals_scored + match.goals_conceded
        filtered_match_count += 1

    if filtered_match_count == 0:
        baseline_goals_per_match = 0.0
    else:
        baseline_goals_per_match = total_goals / filtered_match_count
    baseline_goals_per_team = baseline_goals_per_match / 2.0

    if filtered_match_count < BASELINE_MIN_MATCH_COUNT:
        logger.warning(
            "Baseline computed from only %d matches (< %d); confidence is low "
            "(%d zero-stat matches excluded).",
            filtered_match_count,
            BASELINE_MIN_MATCH_COUNT,
            excluded_zero_stat_matches,
        )

    if filtered_match_count == 0 or baseline_goals_per_team < 1.0:
        logger.warning(
            "Computed baseline_goals_per_team=%.3f is implausibly low; falling "
            "back to BASELINE_GOALS_FLOOR=%.2f (filtered=%d, zero-stat "
            "excluded=%d). Likely a data-coverage gap in the match pool.",
            baseline_goals_per_team,
            BASELINE_GOALS_FLOOR,
            filtered_match_count,
            excluded_zero_stat_matches,
        )
        baseline_goals_per_team = BASELINE_GOALS_FLOOR
        baseline_goals_per_match = BASELINE_GOALS_FLOOR * 2.0

    return TournamentBaseline(
        baseline_goals_per_match=baseline_goals_per_match,
        baseline_goals_per_team=baseline_goals_per_team,
        filtered_match_count=filtered_match_count,
        fifa_points_threshold=float(FIFA_POINTS_FILTER_THRESHOLD),
        excluded_zero_stat_matches=excluded_zero_stat_matches,
    )


# ---------------------------------------------------------------------------
# Poisson / Dixon-Coles
# ---------------------------------------------------------------------------
def poisson_pmf(k: int, lambda_val: float) -> float:
    """Return ``P(X = k)`` for a Poisson distribution with mean ``lambda_val``.

    Thin wrapper over :func:`scipy.stats.poisson.pmf`; this is the per-team
    goal-count probability that the scoreline matrix is built from.
    """
    return float(poisson.pmf(k, lambda_val))


def dixon_coles_correction(
    home_goals: int,
    away_goals: int,
    lambda_home: float,
    lambda_away: float,
    tau: float,
) -> float:
    """Return the Dixon-Coles dependency adjustment for one scoreline.

    A plain independent-Poisson model underestimates low-scoring, correlated
    outcomes (notably 0-0 and 1-1) and mis-weights 1-0 / 0-1. Dixon and Coles
    (1997) introduced a tau-parameterized multiplicative correction applied
    only to those four cells; every other scoreline is unchanged (returns
    ``1.0``).

    Correction factors:
        (0,0): ``1 - lambda_home * lambda_away * tau``
        (0,1): ``1 + lambda_home * tau``
        (1,0): ``1 + lambda_away * tau``
        (1,1): ``1 - tau``
    """
    if home_goals == 0 and away_goals == 0:
        return 1.0 - lambda_home * lambda_away * tau
    if home_goals == 0 and away_goals == 1:
        return 1.0 + lambda_home * tau
    if home_goals == 1 and away_goals == 0:
        return 1.0 + lambda_away * tau
    if home_goals == 1 and away_goals == 1:
        return 1.0 - tau
    return 1.0


def compute_scoreline_matrix(
    xg_a: float,
    xg_b: float,
    max_goals: int = MAX_GOALS_FOR_MATRIX,
    tau: float = DIXON_COLES_TAU,
) -> np.ndarray:
    """Build the joint scoreline probability matrix for a fixture.

    Treats each team's goal count as Poisson with mean equal to its expected
    goals (``xg_a`` for team A, ``xg_b`` for team B), forms the independent
    joint distribution, then applies the Dixon-Coles correction to the
    low-score cells.

    Returns a ``(max_goals + 1) x (max_goals + 1)`` array where
    ``matrix[i][j] = P(team A scores i, team B scores j)``. Because the grid is
    truncated at ``max_goals``, the result is renormalized to sum to ``1.0``.
    """
    size = max_goals + 1
    prob_a = np.array([poisson_pmf(i, xg_a) for i in range(size)])
    prob_b = np.array([poisson_pmf(j, xg_b) for j in range(size)])

    matrix = np.outer(prob_a, prob_b)

    for i in range(size):
        for j in range(size):
            matrix[i, j] *= dixon_coles_correction(i, j, xg_a, xg_b, tau)

    total = matrix.sum()
    if total > 0:
        matrix /= total
    return matrix


def dampen_xg(
    xg_raw: float,
    alpha: float = XG_DAMPENING_ALPHA,
    ceiling: float = XG_HARD_CEILING,
) -> float:
    """Compress raw xG toward realistic ranges.

    Applied to predicted xG immediately before it enters the Poisson
    distribution. The attack x defense multiplicative model compounds with no
    regression toward a realistic ceiling, so extreme mismatches can produce
    implausible xG (e.g. 7+). This is calibration, not a redesign: the Poisson
    and Dixon-Coles math downstream is unchanged.

    Two-step:

    1. Power compression: ``xg_compressed = xg_raw ** alpha``. Shrinks high
       values while leaving moderate values nearly unchanged. ``alpha < 1``
       compresses; ``alpha == 1`` is a no-op.
    2. Hard ceiling: ``min(xg_compressed, ceiling)`` -- a safety cap for
       extreme outputs.

    Returns the dampened xG, always non-negative.
    """
    if xg_raw <= 0:
        return 0.0
    compressed = xg_raw ** alpha
    return min(compressed, ceiling)


def matrix_to_probabilities(matrix: np.ndarray) -> tuple[float, float, float]:
    """Collapse a scoreline matrix into win/draw/loss probabilities.

    Given ``matrix[i][j] = P(A scores i, B scores j)``, sums the cells into:

    - P(A wins): cells where ``i > j`` (A scored more),
    - P(draw): the diagonal where ``i == j``,
    - P(B wins): cells where ``j > i``.

    Returns the triple ``(prob_a_win, prob_draw, prob_b_win)``.
    """
    prob_a_win = float(np.tril(matrix, k=-1).sum())
    prob_draw = float(np.trace(matrix))
    prob_b_win = float(np.triu(matrix, k=1).sum())
    return prob_a_win, prob_draw, prob_b_win
