"""Tests for Phase 4.3 team player display block assembly."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.player_ratings import (  # noqa: E402
    build_matchup_player_display,
    build_team_player_display_block,
)


def _squad_row(squad_no: int, name: str, position: str) -> dict:
    return {
        "squad_no": squad_no,
        "player_name": name,
        "position": position,
        "club": "Club",
        "club_country": "ENG",
    }


def test_manual_ratings_attached_to_squad_not_xi():
    squad = [
        _squad_row(1, "KEEPER", "GK"),
        _squad_row(9, "STRIKER", "FW"),
    ]
    ratings = {
        "source": "manual",
        "listed": [
            {
                "player_id": 0,
                "squad_no": 9,
                "player_name": "STRIKER",
                "avg_rating": 7.5,
                "matches_counted": 2,
                "minutes_share": None,
            }
        ],
        "insufficient_data": [],
    }

    result = build_team_player_display_block(ratings, squad, [])

    assert result["status"] == "partial"
    assert len(result["projected_xi"]) == 1
    assert result["projected_xi"][0]["squad_no"] == 9
    assert result["ratings_source"] == "manual"
    assert len(result["squad"]) == 2
    striker = next(row for row in result["squad"] if row["squad_no"] == 9)
    keeper = next(row for row in result["squad"] if row["squad_no"] == 1)
    assert "avg_rating" not in striker
    assert "avg_rating" not in keeper


def test_matchup_player_display_cache_reuses_team_block():
    client = MagicMock()
    built = {
        "projected_xi": [],
        "bench": [],
        "status": "ok",
        "squad": [{"squad_no": 1}],
    }
    cache: dict[str, dict] = {}

    with patch(
        "src.player_ratings.build_team_player_display_for_code",
        return_value=built,
    ) as mock_build:
        with patch(
            "src.player_ratings.resolve_team_code_for_id",
            side_effect=lambda tid: {10: "ESP", 20: "ESP"}[tid],
        ):
            first = build_matchup_player_display(client, 10, 20, cache=cache)
            second = build_matchup_player_display(client, 10, 20, cache=cache)

    assert mock_build.call_count == 1
    assert first["team_a"] is second["team_a"]
    assert first["team_b"] is second["team_a"]
    assert "ESP" in cache
