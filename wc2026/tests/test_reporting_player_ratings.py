"""Tests for display-only player_ratings in matchup report JSON."""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.models import MatchPrediction, Team, TeamRating  # noqa: E402
from src.player_ratings import build_player_ratings_for_team  # noqa: E402
from src.reporting import (  # noqa: E402
    build_matchup_report,
    matchup_report_to_dict,
    matchup_report_to_json,
)

LINEUPS_FIXTURE_PATH = _PROJECT_ROOT / "data" / "sample_fixture_with_lineups.json"
ESTONIA_ID = 18697
_SCORELINE_MATRIX = [[0.0] * 7 for _ in range(7)]


def _minimal_report(player_ratings: dict[str, Any] | None = None):
    team = Team(
        team_id=1,
        name="Alpha",
        confederation="UEFA",
        fifa_points=1500.0,
        fifa_rank=10,
        is_host=False,
    )
    team_b = replace(team, team_id=2, name="Beta")
    rating = TeamRating(
        team_id=1,
        attack_raw=1.1,
        defense_raw=0.9,
        attack_normalized=1.1,
        defense_normalized=0.9,
        attack_final=1.1,
        defense_final=0.9,
        matches_used=5,
    )
    rating_b = replace(rating, team_id=2)
    prediction = MatchPrediction(
        team_a_id=1,
        team_b_id=2,
        xg_a=1.5,
        xg_b=1.2,
        xg_a_raw=1.6,
        xg_b_raw=1.3,
        prob_a_win=0.45,
        prob_draw=0.25,
        prob_b_win=0.30,
        scoreline_matrix=_SCORELINE_MATRIX,
        most_likely_scoreline=(1, 1),
    )
    report = build_matchup_report(
        team, team_b, [], [], rating, rating_b, prediction
    )
    if player_ratings is not None:
        report = replace(report, player_ratings=player_ratings)
    return report


def _display_block(
    *,
    status: str = "ok",
    xi: list | None = None,
    squad: list | None = None,
) -> dict:
    return {
        "projected_xi": xi or [],
        "bench": [],
        "status": status,
        "squad": squad or [],
        "ratings_source": "sportmonks",
    }


def test_matchup_report_includes_player_ratings_key():
    payload = {
        "team_a": _display_block(
            status="ok",
            xi=[
                {
                    "squad_no": 9,
                    "player_name": "STAR Alpha",
                    "position": "FW",
                    "avg_rating": 7.2,
                    "minutes_share": 0.1,
                    "matches_counted": 3,
                    "match_method": "dob+name",
                }
            ],
            squad=[{"squad_no": 9, "player_name": "STAR Alpha", "position": "FW"}],
        ),
        "team_b": _display_block(status="no_qualifier_data"),
    }
    report = _minimal_report(payload)
    data = matchup_report_to_dict(report)

    assert "player_ratings" in data
    assert data["player_ratings"] == payload
    assert "projected_lineups" in data
    assert data["projected_lineups"]["team_a"]["projected_xi"] == payload["team_a"]["projected_xi"]
    assert data["projected_lineups"]["team_a"]["status"] == "ok"
    assert data["projected_lineups"]["team_b"]["status"] == "no_qualifier_data"
    assert "squad" not in data["projected_lineups"]["team_a"]
    assert data["prediction"]["expected_goals"]["team_a"] == 1.5
    assert "matchup" in data


def test_matchup_report_defaults_empty_player_ratings():
    data = matchup_report_to_dict(_minimal_report())
    empty = {
        "projected_xi": [],
        "bench": [],
        "status": "no_qualifier_data",
        "squad": [],
        "ratings_source": "none",
    }
    assert data["player_ratings"] == {
        "team_a": empty,
        "team_b": empty,
    }
    assert data["projected_lineups"] == {
        "team_a": {
            "status": "no_qualifier_data",
            "ratings_source": "none",
            "projected_xi": [],
            "bench": [],
        },
        "team_b": {
            "status": "no_qualifier_data",
            "ratings_source": "none",
            "projected_xi": [],
            "bench": [],
        },
    }


def test_matchup_report_json_round_trip():
    report = _minimal_report(
        {
            "team_a": _display_block(),
            "team_b": _display_block(),
        }
    )
    parsed = json.loads(matchup_report_to_json(report))
    assert "player_ratings" in parsed
    assert "projected_lineups" in parsed
    assert "team_a" in parsed["projected_lineups"]


def test_build_player_ratings_for_team_fetches_live_fixtures():
    fixture = json.loads(LINEUPS_FIXTURE_PATH.read_text(encoding="utf-8"))
    client = MagicMock()
    client.get_fixtures_for_team.return_value = [{"id": fixture["id"]}]
    client.get_fixture_with_lineups.return_value = fixture

    result = build_player_ratings_for_team(client, ESTONIA_ID)

    client.get_fixtures_for_team.assert_called_once()
    client.get_fixture_with_lineups.assert_called_once_with(fixture["id"])
    assert len(result["listed"]) == 16
    assert result["insufficient_data"] == []


def test_build_player_ratings_for_team_empty_when_no_fixtures():
    client = MagicMock()
    client.get_fixtures_for_team.return_value = []

    result = build_player_ratings_for_team(client, ESTONIA_ID)

    assert result["listed"] == []
    assert result["insufficient_data"] == []
    assert result["source"] == "none"
    client.get_fixture_with_lineups.assert_not_called()
