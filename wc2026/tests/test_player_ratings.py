"""Tests for display-only player rating aggregation (Phase 2)."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.player_ratings import (  # noqa: E402
    MIN_RATED_APPEARANCES,
    aggregate_player_ratings,
    match_rated_players_to_squad,
    normalize_name_tokens,
)
from src.sportmonks_parser import (  # noqa: E402
    PLAYER_STAT_TYPE_IDS,
    parse_fixture_player_ratings,
)

LINEUPS_FIXTURE_PATH = _PROJECT_ROOT / "data" / "sample_fixture_with_lineups.json"
ESTONIA_ID = 18697
KARL_HEIN_ID = 22897102


@pytest.fixture
def estonia_lineups_fixture() -> dict[str, Any]:
    return json.loads(LINEUPS_FIXTURE_PATH.read_text(encoding="utf-8"))


def _lineup_row(
    *,
    player_id: int,
    player_name: str,
    rating: float,
    minutes: float,
    team_id: int = 1,
    dob: str | None = None,
) -> dict[str, Any]:
    row = {
        "team_id": team_id,
        "player_id": player_id,
        "player_name": player_name,
        "details": [
            {
                "type_id": PLAYER_STAT_TYPE_IDS["rating"],
                "data": {"value": rating},
            },
            {
                "type_id": PLAYER_STAT_TYPE_IDS["minutes_played"],
                "data": {"value": minutes},
            },
        ],
    }
    if dob is not None:
        row["player"] = {"date_of_birth": dob}
    return row


def test_aggregate_player_ratings_minutes_weighted_average():
    fixtures = [
        {
            "id": 1001,
            "lineups": [
                _lineup_row(
                    player_id=10, player_name="Alpha", rating=6.0, minutes=90,
                    dob="1990-01-01",
                ),
                _lineup_row(
                    player_id=11, player_name="Beta", rating=7.0, minutes=45,
                    dob="1991-02-02",
                ),
            ],
        },
        {
            "id": 1002,
            "lineups": [
                _lineup_row(player_id=10, player_name="Alpha", rating=8.0, minutes=45),
                _lineup_row(player_id=11, player_name="Beta", rating=5.0, minutes=90),
            ],
        },
    ]

    result = aggregate_player_ratings(team_id=1, fixtures=fixtures)

    alpha = next(r for r in result.listed if r["player_id"] == 10)
    beta = next(r for r in result.listed if r["player_id"] == 11)

    # Alpha: (6*90 + 8*45) / 135 = 6.667...
    assert alpha["matches_counted"] == 2
    assert alpha["dob"] == "1990-01-01"
    assert alpha["avg_rating"] == pytest.approx((6.0 * 90 + 8.0 * 45) / 135)

    # Beta: (7*45 + 5*90) / 135 = 5.667...
    assert beta["matches_counted"] == 2
    assert beta["avg_rating"] == pytest.approx((7.0 * 45 + 5.0 * 90) / 135)

    assert result.listed[0]["player_id"] == 10  # higher avg first
    assert result.insufficient_data == ()


def test_aggregate_player_ratings_lists_single_match_players():
    fixtures = [
        {
            "id": 2001,
            "lineups": [
                _lineup_row(player_id=20, player_name="One Match", rating=6.5, minutes=90),
            ],
        },
        {
            "id": 2002,
            "lineups": [
                _lineup_row(player_id=21, player_name="Two Matches", rating=7.0, minutes=90),
            ],
        },
        {
            "id": 2003,
            "lineups": [
                _lineup_row(player_id=21, player_name="Two Matches", rating=8.0, minutes=90),
            ],
        },
    ]

    result = aggregate_player_ratings(team_id=1, fixtures=fixtures)

    assert len(result.listed) == 2
    assert result.listed[0]["player_name"] == "Two Matches"
    assert result.listed[0]["matches_counted"] == 2
    assert result.listed[1]["player_name"] == "One Match"
    assert result.listed[1]["matches_counted"] == 1
    assert result.insufficient_data == ()


def test_aggregate_player_ratings_preserves_fixture_attribution(
    estonia_lineups_fixture: dict[str, Any],
):
    """Parser rows stay fixture-scoped; aggregation counts distinct fixture_ids."""
    second = copy.deepcopy(estonia_lineups_fixture)
    second["id"] = 19427914

    for row in second["lineups"]:
        if row.get("team_id") != ESTONIA_ID or row.get("player_id") != KARL_HEIN_ID:
            continue
        for detail in row.get("details", []):
            if detail.get("type_id") == PLAYER_STAT_TYPE_IDS["rating"]:
                detail["data"]["value"] = 7.0
            if detail.get("type_id") == PLAYER_STAT_TYPE_IDS["minutes_played"]:
                detail["data"]["value"] = 45

    per_fixture = [
        parse_fixture_player_ratings(estonia_lineups_fixture, ESTONIA_ID),
        parse_fixture_player_ratings(second, ESTONIA_ID),
    ]
    karl_rows = [r for rows in per_fixture for r in rows if r["player_id"] == KARL_HEIN_ID]
    assert {r["fixture_id"] for r in karl_rows} == {19427942, 19427914}

    result = aggregate_player_ratings(
        ESTONIA_ID, [estonia_lineups_fixture, second]
    )
    karl = next(r for r in result.listed if r["player_id"] == KARL_HEIN_ID)
    assert karl["matches_counted"] == 2
    assert karl["avg_rating"] == pytest.approx((6.04 * 90 + 7.0 * 45) / 135)


def test_aggregate_player_ratings_empty_fixtures():
    result = aggregate_player_ratings(team_id=1, fixtures=[])
    assert result.listed == ()
    assert result.insufficient_data == ()


def test_normalize_name_tokens_strips_accents():
    assert normalize_name_tokens("Müller") == {"MULLER"}
    assert normalize_name_tokens("KIM Seunggyu") == {"KIM", "SEUNGGYU"}


def test_match_rated_players_to_squad_dob_and_name():
    squad = [
        {
            "squad_no": "9",
            "player_name": "MESSI Lionel",
            "last_names": "MESSI",
            "name_on_shirt": "MESSi",
            "first_names": "Lionel",
            "dob": "1987-06-24",
        }
    ]
    rated = [
        {
            "player_id": 1,
            "player_name": "Lionel Messi",
            "dob": "1987-06-24",
            "avg_rating": 8.0,
            "matches_counted": 3,
        }
    ]
    matches, unmatched = match_rated_players_to_squad(rated, squad)
    assert len(matches) == 1
    assert matches[0].method == "dob+name"
    assert matches[0].confidence == 1.0
    assert matches[0].flagged is False
    assert unmatched == []


def test_match_rated_players_mononym_via_name_on_shirt():
    squad = [
        {
            "squad_no": "20",
            "player_name": "PEDRI",
            "last_names": "GONZALEZ LOPEZ",
            "name_on_shirt": "PEDRI",
            "first_names": "Pedro",
            "dob": "2002-11-25",
        }
    ]
    rated = [
        {
            "player_id": 1,
            "player_name": "Pedri",
            "dob": "2002-11-25",
            "avg_rating": 8.0,
            "matches_counted": 3,
        }
    ]
    matches, _ = match_rated_players_to_squad(rated, squad)
    assert matches[0].method == "dob+name"
    assert matches[0].flagged is False


def test_match_rated_players_no_name_only_fallback():
    squad = [
        {
            "squad_no": "7",
            "player_name": "PARK Jinseob",
            "last_names": "PARK",
            "name_on_shirt": "JINSEOB",
            "first_names": "Jinseob",
            "dob": "1995-10-23",
        }
    ]
    rated = [
        {
            "player_id": 2,
            "player_name": "Park Yong-Woo",
            "dob": "1993-09-10",
            "avg_rating": 7.0,
            "matches_counted": 3,
        }
    ]
    matches, _ = match_rated_players_to_squad(rated, squad)
    assert matches[0].method == "not_in_current_squad"
    assert matches[0].squad_no is None


def test_match_rated_players_not_in_current_squad():
    rated = [
        {
            "player_id": 3,
            "player_name": "Yunus Musah",
            "dob": "2002-11-29",
            "avg_rating": 7.0,
            "matches_counted": 3,
        }
    ]
    matches, _ = match_rated_players_to_squad(rated, [])
    assert matches[0].method == "not_in_current_squad"


def test_match_rated_players_shared_dob_ambiguous():
    squad = [
        {
            "squad_no": "6",
            "player_name": "MERINO Mikel",
            "last_names": "MERINO",
            "name_on_shirt": "MERINO",
            "first_names": "Mikel",
            "dob": "1996-06-22",
        },
        {
            "squad_no": "16",
            "player_name": "RODRI",
            "last_names": "HERNANDEZ",
            "name_on_shirt": "RODRI",
            "first_names": "Rodrigo",
            "dob": "1996-06-22",
        },
    ]
    rated = [
        {
            "player_id": 4,
            "player_name": "Rodri",
            "dob": "1996-06-22",
            "avg_rating": 8.0,
            "matches_counted": 3,
        }
    ]
    matches, _ = match_rated_players_to_squad(rated, squad)
    assert matches[0].method == "dob+name"
    assert matches[0].squad_player_name == "RODRI"


def test_match_rated_players_shared_dob_unresolved():
    squad = [
        {
            "squad_no": "6",
            "player_name": "MERINO Mikel",
            "last_names": "MERINO",
            "name_on_shirt": "MERINO",
            "first_names": "Mikel",
            "dob": "1996-06-22",
        },
        {
            "squad_no": "16",
            "player_name": "RODRI",
            "last_names": "HERNANDEZ",
            "name_on_shirt": "RODRI",
            "first_names": "Rodrigo",
            "dob": "1996-06-22",
        },
    ]
    rated = [
        {
            "player_id": 5,
            "player_name": "Unknown Player",
            "dob": "1996-06-22",
            "avg_rating": 7.0,
            "matches_counted": 3,
        }
    ]
    matches, _ = match_rated_players_to_squad(rated, squad)
    assert matches[0].method == "unmatched_ambiguous"
