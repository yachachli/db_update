"""Tests for src/sportmonks_parser.py using cached + synthetic fixture JSON.

No API calls. The real Estonia vs Israel fixture is reconstructed from the
cached diagnostic responses: strategy A carries the ``xgfixture`` stats and
strategy C carries the ``participants``, so we merge the two real pieces into
one complete fixture dict.
"""

from __future__ import annotations

import dataclasses
import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Allow `python tests/...` execution by putting the project root on sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sportmonks_parser import (  # noqa: E402
    PLAYER_STAT_TYPE_IDS,
    STAT_TYPE_IDS,
    extract_stat_value,
    parse_fixture_player_ratings,
    parse_fixture_to_match_stats,
)

DIAGNOSTIC_PATH = _PROJECT_ROOT / "data" / "xg_diagnostic_responses.json"
LINEUPS_FIXTURE_PATH = _PROJECT_ROOT / "data" / "sample_fixture_with_lineups.json"

ESTONIA_ID = 18697
ISRAEL_ID = 18657
WC_FINALS_LEAGUE_ID = 732


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _load_estonia_israel_fixture() -> dict[str, Any]:
    """Merge cached strategy A (stats) + strategy C (participants)."""
    diagnostic = json.loads(DIAGNOSTIC_PATH.read_text(encoding="utf-8"))
    strategies = diagnostic["phase2_strategies"]
    stats_data = strategies["A: include=xGFixture"]["response"]["data"]
    participants_data = strategies[
        "C: include=statistics.type;participants;scores"
    ]["response"]["data"]
    fixture = dict(stats_data)
    fixture["participants"] = participants_data["participants"]
    return fixture


@pytest.fixture
def estonia_israel_fixture() -> dict[str, Any]:
    return _load_estonia_israel_fixture()


@pytest.fixture
def estonia_lineups_fixture() -> dict[str, Any]:
    """Cached Estonia qualifier with ``lineups.details`` (fixture 19427942)."""
    return json.loads(LINEUPS_FIXTURE_PATH.read_text(encoding="utf-8"))


def _minimal_fixture(league_id: int = 720) -> dict[str, Any]:
    """Hand-crafted minimal fixture: two teams, goals only (no big chances)."""
    return {
        "id": 999001,
        "starting_at": "2025-01-01 20:00:00",
        "league_id": league_id,
        "participants": [
            {"id": 1, "name": "Alpha", "meta": {"location": "home"}},
            {"id": 2, "name": "Beta", "meta": {"location": "away"}},
        ],
        "xgfixture": [
            {"participant_id": 1, "type_id": STAT_TYPE_IDS["goals"], "data": {"value": 2}},
            {"participant_id": 2, "type_id": STAT_TYPE_IDS["goals"], "data": {"value": 1}},
            # Deliberately NO big_chances_created (type 580) entries.
        ],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_parse_fixture_estonia_perspective(estonia_israel_fixture: dict[str, Any]):
    stats = parse_fixture_to_match_stats(
        estonia_israel_fixture, team_id=ESTONIA_ID, opponent_fifa_points=1400.0
    )

    assert stats.team_id == ESTONIA_ID
    assert stats.opponent_id == ISRAEL_ID
    assert stats.goals_scored == 1  # Estonia scored 1
    assert stats.goals_conceded == 3  # Israel scored 3
    assert stats.xg_created == pytest.approx(0.94, abs=0.01)
    assert stats.venue == "home"  # Estonia hosted; not a finals match
    assert stats.competition_type == "wc_qualifier"

    # No field should be None.
    assert all(v is not None for v in dataclasses.asdict(stats).values())

    # --- Addendum fields ---
    assert isinstance(stats.possession_pct, float)
    assert 0.0 <= stats.possession_pct <= 100.0
    assert stats.opponent_name == "Israel"
    assert stats.outcome == "L"  # Estonia lost
    assert stats.scoreline_str == "1-3 vs Israel (H)"


def test_parse_fixture_israel_perspective(estonia_israel_fixture: dict[str, Any]):
    stats = parse_fixture_to_match_stats(
        estonia_israel_fixture, team_id=ISRAEL_ID, opponent_fifa_points=1300.0
    )

    assert stats.team_id == ISRAEL_ID
    assert stats.opponent_id == ESTONIA_ID
    assert stats.goals_scored == 3  # Israel scored 3
    assert stats.xg_created == pytest.approx(2.58, abs=0.01)
    # xg_conceded prefers Israel's own xGA, which equals Estonia's xG (~0.94).
    assert stats.xg_conceded == pytest.approx(0.94, abs=0.01)
    assert stats.outcome == "W"
    assert stats.opponent_name == "Estonia"


def test_parse_missing_stat_uses_default():
    fixture = _minimal_fixture()
    stats = parse_fixture_to_match_stats(
        fixture, team_id=1, opponent_fifa_points=1200.0
    )

    # Missing big chances default to 0 without raising.
    assert stats.big_chances_created == 0
    assert stats.big_chances_conceded == 0
    assert stats.goals_scored == 2
    assert stats.goals_conceded == 1
    # Missing possession defaults to the neutral 50.0 assumption.
    assert stats.possession_pct == 50.0


def test_parse_fixture_scores_fallback_when_xgfixture_empty():
    """AFC/CAF qualifiers: scores present, xgfixture empty -> goals-only path."""
    fixture = {
        "id": 19312883,
        "starting_at": "2025-03-20 18:00:00",
        "league_id": 714,
        "participants": [
            {"id": 18559, "name": "Jordan", "meta": {"location": "home"}},
            {"id": 18596, "name": "Palestine", "meta": {"location": "away"}},
        ],
        "scores": [
            {
                "description": "CURRENT",
                "participant_id": 18559,
                "score": {"goals": 3, "participant": "home"},
            },
            {
                "description": "CURRENT",
                "participant_id": 18596,
                "score": {"goals": 1, "participant": "away"},
            },
        ],
        "xgfixture": [],
    }
    stats = parse_fixture_to_match_stats(
        fixture, team_id=18559, opponent_fifa_points=1200.0
    )
    assert stats.goals_scored == 3
    assert stats.goals_conceded == 1
    assert stats.xg_created == 0.0
    assert stats.xg_conceded == 0.0


def test_parse_venue_neutral_for_wc_finals():
    fixture = _minimal_fixture(league_id=WC_FINALS_LEAGUE_ID)
    # Team 1 is the "home" participant, but finals are always neutral.
    stats = parse_fixture_to_match_stats(
        fixture, team_id=1, opponent_fifa_points=1500.0
    )
    assert stats.venue == "neutral"
    assert stats.competition_type == "wc_finals"


def test_extract_stat_value_missing_returns_default():
    fixture = _minimal_fixture()
    # type_id 580 absent for participant 1 -> default.
    assert extract_stat_value(fixture, 1, STAT_TYPE_IDS["big_chances_created"]) == 0.0
    assert extract_stat_value(fixture, 1, STAT_TYPE_IDS["goals"]) == 2.0


# ---------------------------------------------------------------------------
# Player ratings (display-only; single-fixture scope)
# ---------------------------------------------------------------------------
def test_parse_fixture_player_ratings_estonia(estonia_lineups_fixture: dict[str, Any]):
    rows = parse_fixture_player_ratings(estonia_lineups_fixture, team_id=ESTONIA_ID)

    assert len(rows) == 16
    assert all(r["fixture_id"] == 19427942 for r in rows)
    assert all(r["player_id"] > 0 for r in rows)
    assert all(isinstance(r["rating"], float) for r in rows)
    assert all(r["minutes_played"] >= 0 for r in rows)
    assert all("dob" in r for r in rows)

    karl = next(r for r in rows if r["player_name"] == "Karl Hein")
    assert karl["player_id"] == 22897102
    assert karl["rating"] == pytest.approx(6.04)
    assert karl["minutes_played"] == pytest.approx(90.0)


def test_parse_fixture_player_ratings_wrong_team(estonia_lineups_fixture: dict[str, Any]):
    assert parse_fixture_player_ratings(estonia_lineups_fixture, team_id=ISRAEL_ID) == []


def test_parse_fixture_player_ratings_no_lineups():
    fixture = _minimal_fixture()
    assert parse_fixture_player_ratings(fixture, team_id=1) == []


def test_parse_fixture_player_ratings_skips_missing_rating():
    fixture = {
        "id": 999002,
        "lineups": [
            {
                "team_id": 1,
                "player_id": 101,
                "player_name": "Rated Starter",
                "player": {"date_of_birth": "1995-03-14"},
                "details": [
                    {
                        "type_id": PLAYER_STAT_TYPE_IDS["rating"],
                        "data": {"value": 7.1},
                    },
                    {
                        "type_id": PLAYER_STAT_TYPE_IDS["minutes_played"],
                        "data": {"value": 90},
                    },
                ],
            },
            {
                "team_id": 1,
                "player_id": 102,
                "player_name": "Bench No Rating",
                "details": [
                    {
                        "type_id": PLAYER_STAT_TYPE_IDS["minutes_played"],
                        "data": {"value": 0},
                    },
                ],
            },
            {
                "team_id": 2,
                "player_id": 201,
                "player_name": "Opponent",
                "details": [
                    {
                        "type_id": PLAYER_STAT_TYPE_IDS["rating"],
                        "data": {"value": 6.5},
                    },
                ],
            },
        ],
    }
    rows = parse_fixture_player_ratings(fixture, team_id=1)
    assert len(rows) == 1
    assert rows[0]["fixture_id"] == 999002
    assert rows[0]["player_name"] == "Rated Starter"
    assert rows[0]["dob"] == "1995-03-14"
