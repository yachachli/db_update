"""Tests for xG-based most-likely scoreline derivation."""

from __future__ import annotations

from src.math_utils import derive_most_likely_scoreline, round_goals_from_xg


def test_mexico_opener_rounds_to_two_one():
    """Mexico 1.51 vs South Africa 0.94 -> 2-1 (gap > 0.5)."""
    assert derive_most_likely_scoreline(1.51, 0.94) == (2, 1)


def test_close_match_stays_draw():
    """Small xG gap keeps rounded draw (USA vs Paraguay-ish)."""
    assert derive_most_likely_scoreline(1.13, 1.06) == (1, 1)


def test_clear_underdog_loses_by_one():
    """Qatar 0.68 vs Switzerland 1.47 -> 0-1 (weak attack stays at 0)."""
    assert derive_most_likely_scoreline(0.68, 1.47) == (0, 1)


def test_favorite_wins_by_one_when_gap_exceeds_threshold():
    """Germany 1.33 vs Curacao 0.68 -> 1-0."""
    assert derive_most_likely_scoreline(1.33, 0.68) == (1, 0)


def test_zero_bucket_requires_point_seventy_five():
    """Sub-1.0 xG only rounds up at 0.75+."""
    assert round_goals_from_xg(0.68) == 0
    assert round_goals_from_xg(0.74) == 0
    assert round_goals_from_xg(0.75) == 1
    assert round_goals_from_xg(0.94) == 1


def test_upper_bucket_rounds_up_above_half():
    """1.x only becomes 2 when strictly above 1.5."""
    assert round_goals_from_xg(1.50) == 1
    assert round_goals_from_xg(1.51) == 2
    assert round_goals_from_xg(2.50) == 2
    assert round_goals_from_xg(2.51) == 3


def test_gap_at_threshold_is_still_draw():
    """Gap exactly 0.5 does not force a one-goal win."""
    assert derive_most_likely_scoreline(1.25, 0.75) == (1, 1)


def test_low_scoring_one_nil():
    """Modest favorite with low totals -> 1-0."""
    assert derive_most_likely_scoreline(0.9, 0.2) == (1, 0)
