"""Calibration checks: FIFA prior + goals-only adjustments."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.aggregation import compute_team_rating
from src.math_utils import compute_baseline_goals
from src.models import MatchStats, Team, TournamentBaseline
from src.pipeline import bootstrap_tournament_pool, predict_matchup_by_id
from src.prediction import predict_match


def _goals_only_match(
    match_id: int,
    team_id: int,
    opponent_id: int,
    *,
    goals_scored: int,
    goals_conceded: int,
    opponent_fifa: float = 1200.0,
) -> MatchStats:
    return MatchStats(
        match_id=match_id,
        date=datetime(2026, 3, match_id),
        team_id=team_id,
        opponent_id=opponent_id,
        opponent_fifa_points=opponent_fifa,
        competition_type="qualifier",
        venue="neutral",
        goals_scored=goals_scored,
        xg_created=0.0,
        big_chances_created=0,
        shots_on_target=0,
        xgot_created=0.0,
        goals_conceded=goals_conceded,
        xg_conceded=0.0,
        big_chances_conceded=0,
        shots_on_target_conceded=0,
        xgot_conceded=0.0,
    )


def _xg_match(
    match_id: int,
    team_id: int,
    opponent_id: int,
    *,
    goals_scored: int,
    goals_conceded: int,
    xg_created: float,
    xg_conceded: float,
    opponent_fifa: float = 1700.0,
) -> MatchStats:
    return MatchStats(
        match_id=match_id,
        date=datetime(2026, 3, match_id),
        team_id=team_id,
        opponent_id=opponent_id,
        opponent_fifa_points=opponent_fifa,
        competition_type="qualifier",
        venue="neutral",
        goals_scored=goals_scored,
        xg_created=xg_created,
        big_chances_created=2,
        shots_on_target=4,
        xgot_created=xg_created,
        goals_conceded=goals_conceded,
        xg_conceded=xg_conceded,
        big_chances_conceded=2,
        shots_on_target_conceded=4,
        xgot_conceded=xg_conceded,
    )


def _baseline() -> TournamentBaseline:
    return TournamentBaseline(
        baseline_goals_per_match=2.7,
        baseline_goals_per_team=1.35,
        filtered_match_count=100,
        fifa_points_threshold=600.0,
    )


def test_goals_only_underdog_not_favored_over_fifa_elite():
    """Jordan-style goals-only window must not make a giant favorite a dog."""
    arg_id, jor_id = 18644, 18559
    argentina = Team(
        team_id=arg_id,
        name="Argentina",
        confederation="CONMEBOL",
        fifa_points=1875.0,
        fifa_rank=1,
        is_host=False,
    )
    jordan = Team(
        team_id=jor_id,
        name="Jordan",
        confederation="AFC",
        fifa_points=1391.0,
        fifa_rank=70,
        is_host=False,
    )
    arg_matches = [
        _xg_match(1, arg_id, 9001, goals_scored=0, goals_conceded=1, xg_created=0.8, xg_conceded=1.2),
        _xg_match(2, arg_id, 9002, goals_scored=2, goals_conceded=0, xg_created=1.9, xg_conceded=0.5),
        _xg_match(3, arg_id, 9003, goals_scored=1, goals_conceded=1, xg_created=1.4, xg_conceded=1.1),
        _xg_match(4, arg_id, 9004, goals_scored=2, goals_conceded=1, xg_created=1.6, xg_conceded=1.0),
        _xg_match(5, arg_id, 9005, goals_scored=3, goals_conceded=0, xg_created=2.1, xg_conceded=0.4),
    ]
    jor_matches = [
        _goals_only_match(6, jor_id, 8001, goals_scored=1, goals_conceded=0, opponent_fifa=1100),
        _goals_only_match(7, jor_id, 8002, goals_scored=0, goals_conceded=0, opponent_fifa=1050),
        _goals_only_match(8, jor_id, 8003, goals_scored=2, goals_conceded=1, opponent_fifa=1150),
        _goals_only_match(9, jor_id, 8004, goals_scored=1, goals_conceded=0, opponent_fifa=1080),
        _goals_only_match(10, jor_id, 8005, goals_scored=1, goals_conceded=1, opponent_fifa=1120),
    ]
    baseline = _baseline()
    rating_arg = compute_team_rating(argentina, arg_matches, baseline)
    rating_jor = compute_team_rating(jordan, jor_matches, baseline)
    prediction = predict_match(rating_arg, rating_jor, baseline)

    assert prediction.prob_a_win > prediction.prob_b_win
    assert prediction.prob_a_win > 0.45
    assert prediction.xg_a > prediction.xg_b


def test_fifa_prior_caps_hot_streak_over_higher_ranked_side():
    """Scotland-style hot form cannot flip Brazil when FIFA ranks Brazil higher."""
    bra_id, sco_id = 18704, 18706
    brazil = Team(
        team_id=bra_id,
        name="Brazil",
        confederation="CONMEBOL",
        fifa_points=1761.0,
        fifa_rank=6,
        is_host=False,
    )
    scotland = Team(
        team_id=sco_id,
        name="Scotland",
        confederation="UEFA",
        fifa_points=1498.0,
        fifa_rank=43,
        is_host=False,
    )
    bra_matches = [
        _xg_match(1, bra_id, 7001, goals_scored=0, goals_conceded=1, xg_created=0.9, xg_conceded=1.4),
        _xg_match(2, bra_id, 7002, goals_scored=2, goals_conceded=0, xg_created=1.8, xg_conceded=0.6),
        _xg_match(3, bra_id, 7003, goals_scored=1, goals_conceded=1, xg_created=1.3, xg_conceded=1.0),
        _xg_match(4, bra_id, 7004, goals_scored=2, goals_conceded=1, xg_created=1.5, xg_conceded=0.9),
        _xg_match(5, bra_id, 7005, goals_scored=1, goals_conceded=0, xg_created=1.2, xg_conceded=0.7),
    ]
    sco_matches = [
        _xg_match(11, sco_id, 6001, goals_scored=4, goals_conceded=2, xg_created=2.8, xg_conceded=1.1),
        _xg_match(12, sco_id, 6002, goals_scored=3, goals_conceded=1, xg_created=2.4, xg_conceded=0.8),
        _xg_match(13, sco_id, 6003, goals_scored=2, goals_conceded=0, xg_created=2.0, xg_conceded=0.5),
        _xg_match(14, sco_id, 6004, goals_scored=2, goals_conceded=1, xg_created=1.9, xg_conceded=0.9),
        _xg_match(15, sco_id, 6005, goals_scored=3, goals_conceded=2, xg_created=2.2, xg_conceded=1.3),
    ]
    baseline = _baseline()
    rating_bra = compute_team_rating(brazil, bra_matches, baseline)
    rating_sco = compute_team_rating(scotland, sco_matches, baseline)
    prediction = predict_match(rating_bra, rating_sco, baseline)

    assert prediction.prob_a_win >= prediction.prob_b_win
    assert prediction.xg_a >= prediction.xg_b * 0.98


@pytest.mark.skipif(
    not (_PROJECT_ROOT / "data" / "cache" / "tournament_pool.json").exists(),
    reason="cached tournament pool required",
)
def test_live_pool_calibration_targets():
    """End-to-end on cached pool: Brazil over Scotland, Argentina over Jordan."""
    pool = bootstrap_tournament_pool()
    sco_bra = json.loads(predict_matchup_by_id(18706, 18704, pool))
    jor_arg = json.loads(predict_matchup_by_id(18559, 18644, pool))

    sco_probs = sco_bra["prediction"]["win_probabilities"]
    jor_probs = jor_arg["prediction"]["win_probabilities"]
    assert sco_probs["team_a_win"] < sco_probs["team_b_win"]
    assert jor_probs["team_a_win"] < jor_probs["team_b_win"]
    assert jor_probs["team_b_win"] > 0.40
