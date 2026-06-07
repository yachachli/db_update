"""Match-history aggregation for the World Cup 2026 predictive model.

Turns a team's last five competitive matches into weighted attack and
defense ratings, applying recency weighting, FIFA-points-based strength
of schedule, and confederation/host adjustments.

Home-soil advantage handling
-----------------------------
Confederation strength and the host bonus represent advantages that make a
team *score more* and *concede fewer* goals. Because the attack rating is a
goals-scored quantity and the defense rating is a goals-*conceded*
(vulnerability) quantity, the two adjustments are applied in opposite
directions:

- **Attack** is *multiplied* by the confederation multiplier and host bonus,
  so a strong-confederation or host team has a *higher* attack rating.
- **Defense** is *divided* by the same factors, so the advantage *lowers*
  the vulnerability number. A strong defensive team has a *lower* defense
  rating.

The net effect: good teams from strong confederations playing on home soil
end up with both a higher attack value and a lower defense value.
"""

from __future__ import annotations

from src.config import (
    CONFEDERATION_MULTIPLIERS,
    DEFENSIVE_STAT_WEIGHTS,
    FIFA_FORM_BLEND_WEIGHT,
    FIFA_PRIOR_EXPONENT,
    FORM_ATTACK_FIFA_CAP_RATIO,
    GOALS_ONLY_DEFENSE_FLOOR_RATIO,
    GOALS_ONLY_FIFA_BLEND,
    GOALS_ONLY_MATCH_THRESHOLD,
    HOST_BONUS,
    OFFENSIVE_STAT_WEIGHTS,
    REFERENCE_FIFA_POINTS,
    STAT_GOAL_CONVERSION_FACTORS,
)
from src.math_utils import match_quality_weight, weighted_average
from src.models import MatchStats, Team, TeamRating, TournamentBaseline

__all__ = [
    "compute_match_weights",
    "compute_weighted_offensive_stats",
    "compute_weighted_defensive_stats",
    "compute_raw_attack_rating",
    "compute_raw_defense_rating",
    "compute_team_rating",
]

# Offensive / defensive stat names, sourced from the configured weight maps so
# the two stay in lockstep with config.py.
_OFFENSIVE_STATS: tuple[str, ...] = tuple(OFFENSIVE_STAT_WEIGHTS)
_DEFENSIVE_STATS: tuple[str, ...] = tuple(DEFENSIVE_STAT_WEIGHTS)


def compute_match_weights(matches: list[MatchStats]) -> list[float]:
    """Return a quality weight for each match, in input order.

    Each weight combines *who* the team played (opponent FIFA points) and
    *where* (venue), via :func:`match_quality_weight`. Heavier weights mean
    the match should count for more when averaging the team's stats.
    """
    return [
        match_quality_weight(match.opponent_fifa_points, match.venue)
        for match in matches
    ]


def compute_weighted_offensive_stats(
    matches: list[MatchStats], weights: list[float]
) -> dict[str, float]:
    """Return the weight-averaged offensive stats across the match window.

    For each offensive stat (``xg_created``, ``goals_scored``,
    ``big_chances_created``, ``shots_on_target``, ``xgot_created``) computes
    the match-quality-weighted average over the supplied matches. Returns a
    dict keyed by stat name.
    """
    return {
        stat: weighted_average(
            [float(getattr(match, stat)) for match in matches], weights
        )
        for stat in _OFFENSIVE_STATS
    }


def compute_weighted_defensive_stats(
    matches: list[MatchStats], weights: list[float]
) -> dict[str, float]:
    """Return the weight-averaged defensive stats across the match window.

    Mirror of :func:`compute_weighted_offensive_stats` for the conceded-side
    stats (``xg_conceded``, ``goals_conceded``, ``big_chances_conceded``,
    ``shots_on_target_conceded``, ``xgot_conceded``). Returns a dict keyed by
    stat name.
    """
    return {
        stat: weighted_average(
            [float(getattr(match, stat)) for match in matches], weights
        )
        for stat in _DEFENSIVE_STATS
    }


def compute_raw_attack_rating(offensive_stats: dict[str, float]) -> float:
    """Collapse weighted offensive stats into a single raw attack number.

    Each weighted-average stat is first converted to a goal-equivalent value
    via ``STAT_GOAL_CONVERSION_FACTORS`` (count-based stats like big chances
    and shots on target are scaled down by ~3, since roughly one in three
    becomes a goal), then combined using ``OFFENSIVE_STAT_WEIGHTS`` (which sum
    to 1.0). The conversion keeps count stats from dominating the rating purely
    because their raw magnitudes are larger, so ``attack_raw`` lands in genuine
    goal units. Higher is better.
    """
    return sum(
        offensive_stats[stat] * STAT_GOAL_CONVERSION_FACTORS[stat] * weight
        for stat, weight in OFFENSIVE_STAT_WEIGHTS.items()
    )


def _is_goals_only_match(match: MatchStats) -> bool:
    """True when a match has no xG/xGOT and ratings rely on goals-only fallback."""
    return (
        match.xg_created == 0.0
        and match.xg_conceded == 0.0
        and match.xgot_created == 0.0
        and match.xgot_conceded == 0.0
    )


def _goals_only_fraction(matches: list[MatchStats]) -> float:
    if not matches:
        return 0.0
    return sum(1 for match in matches if _is_goals_only_match(match)) / len(matches)


def _fifa_prior_ratings(team: Team, advantage: float) -> tuple[float, float]:
    """Return FIFA-implied attack and defense vulnerability (conf/host scaled)."""
    ratio = team.fifa_points / REFERENCE_FIFA_POINTS
    fifa_attack = (ratio ** FIFA_PRIOR_EXPONENT) * advantage
    fifa_defense = (ratio ** -FIFA_PRIOR_EXPONENT) / advantage
    return fifa_attack, fifa_defense


def _blend_form_with_fifa_prior(
    attack_form: float,
    defense_form: float,
    fifa_attack: float,
    fifa_defense: float,
    *,
    form_weight: float,
) -> tuple[float, float]:
    """Shrink form ratings toward FIFA-implied priors."""
    fifa_weight = 1.0 - form_weight
    attack = form_weight * attack_form + fifa_weight * fifa_attack
    defense = form_weight * defense_form + fifa_weight * fifa_defense
    return attack, defense


def _apply_goals_only_calibration(
    attack_final: float,
    defense_final: float,
    fifa_attack: float,
    fifa_defense: float,
    goals_only_fraction: float,
) -> tuple[float, float]:
    """Pull thin-xG windows toward FIFA and floor artificially low vulnerability."""
    if goals_only_fraction < GOALS_ONLY_MATCH_THRESHOLD:
        return attack_final, defense_final

    attack_final, defense_final = _blend_form_with_fifa_prior(
        attack_final,
        defense_final,
        fifa_attack,
        fifa_defense,
        form_weight=1.0 - GOALS_ONLY_FIFA_BLEND,
    )
    defense_floor = fifa_defense * GOALS_ONLY_DEFENSE_FLOOR_RATIO
    if defense_final < defense_floor:
        defense_final = defense_floor
    return attack_final, defense_final


def _cap_attack_vs_fifa(attack_final: float, fifa_attack: float) -> float:
    """Prevent short hot streaks from dwarfing long-run FIFA strength."""
    cap = fifa_attack * FORM_ATTACK_FIFA_CAP_RATIO
    return min(attack_final, cap)


def compute_raw_defense_rating(defensive_stats: dict[str, float]) -> float:
    """Collapse weighted defensive stats into a single raw defense number.

    Mirror of :func:`compute_raw_attack_rating`: each weighted-average conceded
    stat is converted to goal-equivalent units via
    ``STAT_GOAL_CONVERSION_FACTORS`` before being combined with
    ``DEFENSIVE_STAT_WEIGHTS`` (which sum to 1.0).

    Note: this is a *vulnerability* measure — a higher value means the team
    tends to concede *more* goals. The prediction step interprets it
    accordingly (a strong defense is a *low* number).
    """
    return sum(
        defensive_stats[stat] * STAT_GOAL_CONVERSION_FACTORS[stat] * weight
        for stat, weight in DEFENSIVE_STAT_WEIGHTS.items()
    )


def compute_team_rating(
    team: Team, matches: list[MatchStats], baseline: TournamentBaseline
) -> TeamRating:
    """Run the full aggregation pipeline for one team.

    Steps:
        1. Compute per-match quality weights.
        2. Weight-average the offensive and defensive stats.
        3. Reduce each to a raw attack / defense rating.
        4. Normalize both against the tournament per-team goal baseline.
        5. Apply confederation and host adjustments.
        6. Blend toward FIFA-implied priors and cap hot-streak attack inflation.
        7. For goals-only match windows, pull further toward FIFA and floor
           artificially low defensive vulnerability.

    Home-soil advantage (see module docstring): the confederation multiplier
    and host bonus *multiply* the attack rating (more goals scored) but
    *divide* the defense rating (fewer goals conceded), so the same advantage
    raises attack and lowers defensive vulnerability.

    ``matches_used`` is set to ``len(matches)`` so callers can flag teams with
    a thin match window as low-confidence.
    """
    weights = compute_match_weights(matches)
    offensive_stats = compute_weighted_offensive_stats(matches, weights)
    defensive_stats = compute_weighted_defensive_stats(matches, weights)

    attack_raw = compute_raw_attack_rating(offensive_stats)
    defense_raw = compute_raw_defense_rating(defensive_stats)

    per_team_baseline = baseline.baseline_goals_per_team
    if per_team_baseline == 0:
        attack_normalized = 0.0
        defense_normalized = 0.0
    else:
        attack_normalized = attack_raw / per_team_baseline
        defense_normalized = defense_raw / per_team_baseline

    confederation_mult = CONFEDERATION_MULTIPLIERS.get(team.confederation, 1.0)
    host_mult = HOST_BONUS if team.is_host else 1.0
    advantage = confederation_mult * host_mult

    attack_form = attack_normalized * advantage
    defense_form = defense_normalized / advantage

    fifa_attack, fifa_defense = _fifa_prior_ratings(team, advantage)
    attack_final, defense_final = _blend_form_with_fifa_prior(
        attack_form,
        defense_form,
        fifa_attack,
        fifa_defense,
        form_weight=1.0 - FIFA_FORM_BLEND_WEIGHT,
    )
    attack_final = _cap_attack_vs_fifa(attack_final, fifa_attack)
    attack_final, defense_final = _apply_goals_only_calibration(
        attack_final,
        defense_final,
        fifa_attack,
        fifa_defense,
        _goals_only_fraction(matches),
    )

    return TeamRating(
        team_id=team.team_id,
        attack_raw=attack_raw,
        defense_raw=defense_raw,
        attack_normalized=attack_normalized,
        defense_normalized=defense_normalized,
        attack_final=attack_final,
        defense_final=defense_final,
        matches_used=len(matches),
    )
