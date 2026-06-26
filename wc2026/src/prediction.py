"""Match-outcome prediction for the World Cup 2026 predictive model.

Combines two teams' aggregated attack/defense ratings into Poisson goal
expectancies and produces calibrated win/draw/loss probabilities (with
the Dixon-Coles correction) for a given fixture.

World Cup matches are played at a neutral venue for every team, and the
host bonus has already been baked into each team's rating during
aggregation, so no additional venue multiplier is applied here.
"""

from __future__ import annotations

from src import config
from src.math_utils import (
    compute_scoreline_matrix,
    dampen_xg,
    matrix_to_probabilities,
    most_likely_scoreline_from_matrix,
)
from src.models import MatchPrediction, TeamRating, TournamentBaseline

__all__ = ["predict_match", "format_prediction"]


def predict_match(
    rating_a: TeamRating,
    rating_b: TeamRating,
    baseline: TournamentBaseline,
) -> MatchPrediction:
    """Predict the outcome distribution for a single fixture.

    Each team's expected goals is its attacking strength multiplied by the
    opponent's defensive vulnerability, scaled by the tournament's per-team
    goal baseline::

        xg_a = rating_a.attack_final * rating_b.defense_final * baseline_per_team
        xg_b = rating_b.attack_final * rating_a.defense_final * baseline_per_team

    Because ``defense_final`` is a vulnerability measure (higher means more
    goals conceded), a leaky opponent inflates a team's xG while a stingy one
    suppresses it. Those expectancies drive a Dixon-Coles-corrected Poisson
    scoreline matrix, which is collapsed into win/draw/loss probabilities and
    scanned for the single most likely scoreline.

    The raw expectancies are passed through :func:`~src.math_utils.dampen_xg`
    before driving the Poisson matrix, compressing implausible values from
    extreme mismatches. The Poisson/Dixon-Coles math itself is unchanged.

    No venue multiplier is applied: World Cup matches are neutral for everyone
    and the host advantage is already in the ratings.
    """
    per_team_baseline = baseline.baseline_goals_per_team

    xg_a_raw = rating_a.attack_final * rating_b.defense_final * per_team_baseline
    xg_b_raw = rating_b.attack_final * rating_a.defense_final * per_team_baseline

    xg_a = dampen_xg(xg_a_raw) * config.TOURNAMENT_GOAL_MULTIPLIER
    xg_b = dampen_xg(xg_b_raw) * config.TOURNAMENT_GOAL_MULTIPLIER

    matrix = compute_scoreline_matrix(xg_a, xg_b)
    prob_a_win, prob_draw, prob_b_win = matrix_to_probabilities(matrix)

    # Read the headline scoreline straight from the same matrix, restricted to
    # the modal win/draw/loss outcome so the score can never contradict the
    # percentages (e.g. a "1-1" shown next to a 46% favorite).
    modal_outcome = ("a", "d", "b")[
        max(
            range(3),
            key=(prob_a_win, prob_draw, prob_b_win).__getitem__,
        )
    ]
    most_likely_scoreline = most_likely_scoreline_from_matrix(
        matrix, outcome=modal_outcome
    )

    return MatchPrediction(
        team_a_id=rating_a.team_id,
        team_b_id=rating_b.team_id,
        xg_a=xg_a,
        xg_b=xg_b,
        prob_a_win=prob_a_win,
        prob_draw=prob_draw,
        prob_b_win=prob_b_win,
        scoreline_matrix=matrix.tolist(),
        most_likely_scoreline=most_likely_scoreline,
        xg_a_raw=xg_a_raw,
        xg_b_raw=xg_b_raw,
    )


def format_prediction(
    prediction: MatchPrediction,
    team_a_name: str,
    team_b_name: str,
) -> str:
    """Render a :class:`MatchPrediction` as a human-readable summary.

    Includes each team's expected goals, the win/draw/loss percentages, and
    the single most likely scoreline. Intended for CLI/log output, not for
    machine consumption.
    """
    score_a, score_b = prediction.most_likely_scoreline
    return (
        f"{team_a_name} vs {team_b_name}\n"
        f"  Expected goals: {prediction.xg_a:.2f} - {prediction.xg_b:.2f}\n"
        f"  {team_a_name} win: {prediction.prob_a_win:.1%}\n"
        f"  Draw: {prediction.prob_draw:.1%}\n"
        f"  {team_b_name} win: {prediction.prob_b_win:.1%}\n"
        f"  Most likely scoreline: {team_a_name} {score_a}-{score_b} {team_b_name}"
    )
