"""Tests for schedule_service. No live API or parquet required."""
import pytest
from app.services.schedule_service import parse_fanmatch_game, parse_fanmatch_prediction


def test_parse_fanmatch_game_at():
    out = parse_fanmatch_game("14 Alabama at 32 Georgia SEC")
    assert out is not None
    assert out["away_team"] == "Alabama"
    assert out["away_rank"] == 14
    assert out["home_team"] == "Georgia"
    assert out["home_rank"] == 32
    assert out["conference"] == "SEC"
    assert out["is_neutral"] is False


def test_parse_fanmatch_game_multi_word():
    out = parse_fanmatch_game("36 Clemson at 28 North Carolina ACC")
    assert out is not None
    assert out["away_team"] == "Clemson"
    assert out["home_team"] == "North Carolina"
    assert out["conference"] == "ACC"


def test_parse_fanmatch_game_neutral():
    out = parse_fanmatch_game("5 Duke vs. 12 Kentucky")
    assert out is not None
    assert out["away_team"] == "Duke"
    assert out["home_team"] == "Kentucky"
    assert out["is_neutral"] is True


def test_parse_fanmatch_game_invalid():
    assert parse_fanmatch_game("") is None
    assert parse_fanmatch_game("not a valid format") is None


def test_parse_fanmatch_prediction():
    out = parse_fanmatch_prediction("Alabama 92-91 (55%) [75]")
    assert out is not None
    assert out["favored_team"] == "Alabama"
    assert out["predicted_score_fav"] == 92
    assert out["predicted_score_dog"] == 91
    assert out["predicted_margin"] == 1
    assert out["win_probability"] == 0.55


def test_parse_fanmatch_prediction_invalid():
    assert parse_fanmatch_prediction("") is None
    assert parse_fanmatch_prediction("no numbers") is None
