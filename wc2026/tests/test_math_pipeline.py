"""End-to-end validation of the model math on synthetic data (no API).

Builds two hypothetical teams and five plausible matches each, then runs the
full pipeline -- baseline -> per-team ratings -> match prediction -- and
asserts the output is internally consistent and directionally sensible.

Run via pytest, or standalone for an eyeball check:

    python tests/test_math_pipeline.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Allow `python tests/test_math_pipeline.py` to find the `src` package by
# putting the project root (this file's parent's parent) on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.aggregation import (
    compute_match_weights,
    compute_raw_attack_rating,
    compute_team_rating,
    compute_weighted_offensive_stats,
)
from src.config import XG_HARD_CEILING
from src.math_utils import compute_baseline_goals
from src.models import MatchStats, Team, TeamRating, TournamentBaseline
from src.prediction import format_prediction, predict_match

# ---------------------------------------------------------------------------
# Synthetic fixtures
# ---------------------------------------------------------------------------
TEAM_A_ID = 1  # strong European side
TEAM_B_ID = 2  # mid-tier CONCACAF host


def _make_match(
    match_id: int,
    team_id: int,
    opponent_id: int,
    opponent_fifa_points: float,
    venue: str,
    *,
    goals_scored: int,
    xg_created: float,
    big_chances_created: int,
    shots_on_target: int,
    xgot_created: float,
    goals_conceded: int,
    xg_conceded: float,
    big_chances_conceded: int,
    shots_on_target_conceded: int,
    xgot_conceded: float,
) -> MatchStats:
    """Construct a MatchStats with an arbitrary friendly competition/date."""
    return MatchStats(
        match_id=match_id,
        date=datetime(2026, 3, match_id),
        team_id=team_id,
        opponent_id=opponent_id,
        opponent_fifa_points=opponent_fifa_points,
        competition_type="friendly",
        venue=venue,
        goals_scored=goals_scored,
        xg_created=xg_created,
        big_chances_created=big_chances_created,
        shots_on_target=shots_on_target,
        xgot_created=xgot_created,
        goals_conceded=goals_conceded,
        xg_conceded=xg_conceded,
        big_chances_conceded=big_chances_conceded,
        shots_on_target_conceded=shots_on_target_conceded,
        xgot_conceded=xgot_conceded,
    )


def make_team_a() -> Team:
    """Strong European team: UEFA, 1850 FIFA pts, not a host."""
    return Team(
        team_id=TEAM_A_ID,
        name="Atlantica",
        confederation="UEFA",
        fifa_points=1850.0,
        fifa_rank=4,
        is_host=False,
    )


def make_team_b() -> Team:
    """Mid-tier CONCACAF team: 1300 FIFA pts, tournament host."""
    return Team(
        team_id=TEAM_B_ID,
        name="Costa Verde",
        confederation="CONCACAF",
        fifa_points=1300.0,
        fifa_rank=38,
        is_host=True,
    )


def make_team_a_matches() -> list[MatchStats]:
    """Team A: mostly wins vs strong opponents (~2.0-2.5 xG, concedes 0.5-1.0)."""
    return [
        _make_match(
            1, TEAM_A_ID, 101, 1780.0, "home",
            goals_scored=3, xg_created=2.4, big_chances_created=4,
            shots_on_target=7, xgot_created=2.6,
            goals_conceded=0, xg_conceded=0.6, big_chances_conceded=1,
            shots_on_target_conceded=2, xgot_conceded=0.5,
        ),
        _make_match(
            2, TEAM_A_ID, 102, 1820.0, "away",
            goals_scored=2, xg_created=2.1, big_chances_created=3,
            shots_on_target=6, xgot_created=2.2,
            goals_conceded=1, xg_conceded=0.9, big_chances_conceded=2,
            shots_on_target_conceded=3, xgot_conceded=0.8,
        ),
        _make_match(
            3, TEAM_A_ID, 103, 1650.0, "neutral",
            goals_scored=2, xg_created=2.5, big_chances_created=4,
            shots_on_target=8, xgot_created=2.7,
            goals_conceded=0, xg_conceded=0.5, big_chances_conceded=1,
            shots_on_target_conceded=2, xgot_conceded=0.4,
        ),
        _make_match(
            4, TEAM_A_ID, 104, 1900.0, "away",
            goals_scored=2, xg_created=2.0, big_chances_created=3,
            shots_on_target=5, xgot_created=2.1,
            goals_conceded=1, xg_conceded=1.0, big_chances_conceded=2,
            shots_on_target_conceded=4, xgot_conceded=1.0,
        ),
        _make_match(
            5, TEAM_A_ID, 105, 1700.0, "home",
            goals_scored=3, xg_created=2.3, big_chances_created=4,
            shots_on_target=7, xgot_created=2.5,
            goals_conceded=1, xg_conceded=0.8, big_chances_conceded=2,
            shots_on_target_conceded=3, xgot_conceded=0.7,
        ),
    ]


def make_team_b_matches() -> list[MatchStats]:
    """Team B: mixed results vs medium opponents (~1.2-1.5 xG, concedes 1.0-1.5)."""
    return [
        _make_match(
            6, TEAM_B_ID, 201, 1320.0, "home",
            goals_scored=2, xg_created=1.5, big_chances_created=2,
            shots_on_target=5, xgot_created=1.6,
            goals_conceded=1, xg_conceded=1.1, big_chances_conceded=2,
            shots_on_target_conceded=4, xgot_conceded=1.2,
        ),
        _make_match(
            7, TEAM_B_ID, 202, 1280.0, "away",
            goals_scored=1, xg_created=1.2, big_chances_created=1,
            shots_on_target=3, xgot_created=1.1,
            goals_conceded=1, xg_conceded=1.4, big_chances_conceded=3,
            shots_on_target_conceded=5, xgot_conceded=1.5,
        ),
        _make_match(
            8, TEAM_B_ID, 203, 1350.0, "neutral",
            goals_scored=1, xg_created=1.3, big_chances_created=2,
            shots_on_target=4, xgot_created=1.4,
            goals_conceded=2, xg_conceded=1.5, big_chances_conceded=3,
            shots_on_target_conceded=5, xgot_conceded=1.6,
        ),
        _make_match(
            9, TEAM_B_ID, 204, 1240.0, "home",
            goals_scored=2, xg_created=1.4, big_chances_created=2,
            shots_on_target=5, xgot_created=1.5,
            goals_conceded=1, xg_conceded=1.0, big_chances_conceded=1,
            shots_on_target_conceded=3, xgot_conceded=1.0,
        ),
        _make_match(
            10, TEAM_B_ID, 205, 1310.0, "away",
            goals_scored=1, xg_created=1.2, big_chances_created=1,
            shots_on_target=3, xgot_created=1.2,
            goals_conceded=2, xg_conceded=1.3, big_chances_conceded=3,
            shots_on_target_conceded=6, xgot_conceded=1.4,
        ),
    ]


def _synthesize_opponents(matches: list[MatchStats]) -> dict[int, Team]:
    """Build placeholder Team entries for each opponent from match data.

    ``compute_baseline_goals`` looks up both sides of a match in the teams
    dict to measure their FIFA-points gap, so the opponents must be present.
    Their FIFA points come straight from each match's ``opponent_fifa_points``;
    the other fields are irrelevant to the baseline and use neutral defaults.
    """
    opponents: dict[int, Team] = {}
    for match in matches:
        if match.opponent_id not in opponents:
            opponents[match.opponent_id] = Team(
                team_id=match.opponent_id,
                name=f"Opponent {match.opponent_id}",
                confederation="UEFA",
                fifa_points=match.opponent_fifa_points,
                fifa_rank=0,
                is_host=False,
            )
    return opponents


def run_pipeline():
    """Run the full pipeline and return (team_a, team_b, rating_a, rating_b, prediction)."""
    team_a = make_team_a()
    team_b = make_team_b()
    matches_a = make_team_a_matches()
    matches_b = make_team_b_matches()

    all_matches = matches_a + matches_b

    # The baseline calc needs every team referenced by a match -- both the two
    # teams under test and all of their opponents.
    teams = {team_a.team_id: team_a, team_b.team_id: team_b}
    teams.update(_synthesize_opponents(all_matches))

    baseline = compute_baseline_goals(all_matches, teams)
    rating_a = compute_team_rating(team_a, matches_a, baseline)
    rating_b = compute_team_rating(team_b, matches_b, baseline)
    prediction = predict_match(rating_a, rating_b, baseline)

    return team_a, team_b, rating_a, rating_b, prediction


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
def test_full_pipeline_directional_and_consistent():
    """The strong team should dominate and probabilities must be coherent."""
    _, _, rating_a, rating_b, prediction = run_pipeline()

    # Both teams used the full 5-match window.
    assert rating_a.matches_used == 5
    assert rating_b.matches_used == 5

    # Directional: A attacks better and defends better (lower = better defense).
    assert rating_a.attack_final > rating_b.attack_final
    assert rating_a.defense_final < rating_b.defense_final

    # A is the clear favourite.
    assert prediction.prob_a_win > prediction.prob_b_win

    # Probabilities form a valid distribution.
    total = prediction.prob_a_win + prediction.prob_draw + prediction.prob_b_win
    assert abs(total - 1.0) < 0.001

    # Expected goals are positive and in a realistic range.
    assert 0.3 < prediction.xg_a < 4.0
    assert 0.3 < prediction.xg_b < 4.0

    # The xG matchup should also favour A.
    assert prediction.xg_a > prediction.xg_b


# ---------------------------------------------------------------------------
# xG dampening
# ---------------------------------------------------------------------------
def _baseline(per_team: float) -> TournamentBaseline:
    """A minimal baseline with a chosen per-team scaling factor."""
    return TournamentBaseline(
        baseline_goals_per_match=per_team * 2.0,
        baseline_goals_per_team=per_team,
        filtered_match_count=20,
        fifa_points_threshold=600.0,
    )


def _rating(team_id: int, attack_final: float, defense_final: float) -> TeamRating:
    """A TeamRating with only the *_final fields that predict_match consumes."""
    return TeamRating(
        team_id=team_id,
        attack_raw=attack_final,
        defense_raw=defense_final,
        attack_normalized=attack_final,
        defense_normalized=defense_final,
        attack_final=attack_final,
        defense_final=defense_final,
        matches_used=5,  # avoid the low-confidence UserWarning
    )


def test_extreme_mismatch_dampened_correctly():
    """An overpowered attack should be compressed below the hard ceiling."""
    strong = _rating(1, attack_final=2.5, defense_final=0.5)
    weak = _rating(2, attack_final=0.7, defense_final=1.4)
    # per_team=1.8 -> raw xg_a = 2.5 * 1.4 * 1.8 = 6.3, well above the ceiling.
    prediction = predict_match(strong, weak, _baseline(1.8))

    # The raw value would have been unrealistic...
    assert prediction.xg_a_raw > XG_HARD_CEILING
    # ...but the dampened value that drove the prediction is capped.
    assert prediction.xg_a <= XG_HARD_CEILING
    # Dampening visibly reduced the value.
    assert prediction.xg_a < prediction.xg_a_raw
    # Probabilities still form a valid distribution.
    total = prediction.prob_a_win + prediction.prob_draw + prediction.prob_b_win
    assert abs(total - 1.0) < 0.001


def test_moderate_matchup_minimally_dampened():
    """In the normal range, compression should be barely perceptible."""
    team_a = _rating(1, attack_final=1.0, defense_final=1.0)
    team_b = _rating(2, attack_final=1.0, defense_final=1.0)
    # per_team=1.5 -> raw xg = 1.0 * 1.0 * 1.5 = 1.5 for both sides.
    prediction = predict_match(team_a, team_b, _baseline(1.5))

    assert abs(prediction.xg_a - prediction.xg_a_raw) < 0.1
    assert abs(prediction.xg_b - prediction.xg_b_raw) < 0.1


# ---------------------------------------------------------------------------
# Baseline data-quality + goal-unit aggregation
# ---------------------------------------------------------------------------
def test_baseline_excludes_zero_stat_matches():
    """All-zero-stat matches are dropped; baseline uses only real matches."""
    team_id, opp_id = 1, 2
    teams = {
        team_id: Team(team_id, "A", "UEFA", 1500.0, 10, False),
        opp_id: Team(opp_id, "B", "UEFA", 1500.0, 11, False),
    }

    # 5 real matches: each totals 3 goals (2 scored, 1 conceded).
    real = [
        _make_match(
            i, team_id, opp_id, 1500.0, "neutral",
            goals_scored=2, xg_created=2.0, big_chances_created=4,
            shots_on_target=6, xgot_created=2.0,
            goals_conceded=1, xg_conceded=1.0, big_chances_conceded=2,
            shots_on_target_conceded=4, xgot_conceded=1.0,
        )
        for i in range(1, 6)
    ]
    # 5 zero-stat matches (missing data, not real goalless games).
    zeros = [
        _make_match(
            i, team_id, opp_id, 1500.0, "neutral",
            goals_scored=0, xg_created=0.0, big_chances_created=0,
            shots_on_target=0, xgot_created=0.0,
            goals_conceded=0, xg_conceded=0.0, big_chances_conceded=0,
            shots_on_target_conceded=0, xgot_conceded=0.0,
        )
        for i in range(6, 11)
    ]

    baseline = compute_baseline_goals(real + zeros, teams)

    assert baseline.excluded_zero_stat_matches == 5
    assert baseline.filtered_match_count == 5
    # 5 real matches x 3 goals each / 5 = 3.0 per match => 1.5 per team. The
    # >= 1.0 value is real (not the floor), so it reflects the 5 real matches.
    assert abs(baseline.baseline_goals_per_match - 3.0) < 1e-9
    assert abs(baseline.baseline_goals_per_team - 1.5) < 1e-9


def test_attack_raw_in_goal_units():
    """With goal-conversion applied, attack_raw lands in genuine goal units."""
    team_id = 1
    matches = [
        _make_match(
            i, team_id, 100 + i, 1500.0, "neutral",
            goals_scored=2, xg_created=2.0, big_chances_created=6,
            shots_on_target=8, xgot_created=2.0,
            goals_conceded=1, xg_conceded=1.0, big_chances_conceded=3,
            shots_on_target_conceded=4, xgot_conceded=1.0,
        )
        for i in range(1, 6)
    ]
    weights = compute_match_weights(matches)
    offensive = compute_weighted_offensive_stats(matches, weights)
    attack_raw = compute_raw_attack_rating(offensive)

    # Count stats (big chances 6, sot 8) no longer dominate: the rating sits
    # in real goal units rather than the inflated ~3+ it would be without
    # conversion.
    assert 1.5 <= attack_raw <= 2.5


# ---------------------------------------------------------------------------
# Standalone sanity check
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    team_a, team_b, rating_a, rating_b, prediction = run_pipeline()

    print("=" * 60)
    print("World Cup 2026 model -- synthetic pipeline check")
    print("=" * 60)
    print(
        f"{team_a.name:<12} attack_final={rating_a.attack_final:.3f}  "
        f"defense_final={rating_a.defense_final:.3f}  "
        f"(used {rating_a.matches_used} matches)"
    )
    print(
        f"{team_b.name:<12} attack_final={rating_b.attack_final:.3f}  "
        f"defense_final={rating_b.defense_final:.3f}  "
        f"(used {rating_b.matches_used} matches)"
    )
    print("-" * 60)
    print(format_prediction(prediction, team_a.name, team_b.name))
    print("=" * 60)

    # Mirror the assertions so a direct run also acts as a pass/fail check.
    test_full_pipeline_directional_and_consistent()
    test_extreme_mismatch_dampened_correctly()
    test_moderate_matchup_minimally_dampened()
    test_baseline_excludes_zero_stat_matches()
    test_attack_raw_in_goal_units()
    print("All assertions passed.")
