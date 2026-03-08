"""
Tests for BracketIQ prediction model. No KenPom/network calls.
"""

import pytest
from app.models.schemas import TeamProfile
from app.models.prediction import BracketIQModel, MARGIN_SIGMA, HOME_COURT_ADVANTAGE


def _profile(name: str, adj_oe: float = 110, adj_de: float = 95, adj_tempo: float = 68, **kwargs) -> TeamProfile:
    return TeamProfile(
        name=name,
        conference="TEST",
        kenpom_rank=1,
        adj_oe=adj_oe,
        adj_de=adj_de,
        adj_em=adj_oe - adj_de,
        adj_tempo=adj_tempo,
        **kwargs,
    )


def test_baseline_margin_neutral():
    """Stronger team (higher AdjEM) should have positive margin and win prob > 0.5."""
    a = _profile("A", adj_oe=115, adj_de=95)
    b = _profile("B", adj_oe=105, adj_de=100)
    model = BracketIQModel()
    out = model.predict_matchup(a, b, neutral=True)
    assert out.predicted_margin > 0
    assert out.win_prob_a > 0.5
    assert out.win_prob_a + out.win_prob_b == pytest.approx(1.0)
    assert out.kenpom_baseline is not None
    assert out.team_a == "A" and out.team_b == "B"


def test_home_court():
    """With neutral=False, margin should increase for team_a (home)."""
    a = _profile("A", adj_oe=110, adj_de=100)
    b = _profile("B", adj_oe=110, adj_de=100)
    model = BracketIQModel()
    neutral_out = model.predict_matchup(a, b, neutral=True)
    home_out = model.predict_matchup(a, b, neutral=False)
    assert home_out.predicted_margin > neutral_out.predicted_margin
    assert home_out.win_prob_a > neutral_out.win_prob_a


def test_weights_offense():
    """Increasing offense weight should amplify offensive edge."""
    a = _profile("A", adj_oe=120, adj_de=100)
    b = _profile("B", adj_oe=100, adj_de=100)
    base = BracketIQModel().predict_matchup(a, b, neutral=True)
    heavy_off = BracketIQModel(weights={"offense": 1.5}).predict_matchup(a, b, neutral=True)
    assert heavy_off.predicted_margin > base.predicted_margin
    assert heavy_off.win_prob_a > base.win_prob_a
