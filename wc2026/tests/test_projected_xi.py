"""Tests for build_projected_xi (Phase 4.2)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from datetime import date

from src.player_ratings import (  # noqa: E402
    _flatten_projected_lineup_rows,
    build_projected_xi,
)


def _squad_row(squad_no: int, player_name: str, position: str) -> dict:
    return {
        "squad_no": squad_no,
        "player_name": player_name,
        "position": position,
        "last_names": player_name.split()[-1],
        "name_on_shirt": player_name.split()[-1],
    }


def _rating(
    player_id: int,
    *,
    avg_rating: float,
    minutes_share: float | None,
    matches_counted: int = 3,
) -> dict:
    return {
        "player_id": player_id,
        "player_name": f"SM {player_id}",
        "avg_rating": avg_rating,
        "minutes_share": minutes_share,
        "matches_counted": matches_counted,
        "source": "sportmonks",
    }


def _map_row(player_id: int, squad_no: int, method: str = "dob+name") -> dict:
    return {
        "sportmonks_player_id": player_id,
        "squad_no": squad_no,
        "match_method": method,
    }


def _team_ratings(*players: dict) -> dict:
    return {
        "source": "sportmonks",
        "listed": list(players),
        "insufficient_data": [],
    }


def test_build_projected_xi_fills_full_template():
    squad = [
        _squad_row(1, "KEEPER A", "GK"),
        _squad_row(2, "DEF A", "DF"),
        _squad_row(3, "DEF B", "DF"),
        _squad_row(4, "DEF C", "DF"),
        _squad_row(5, "DEF D", "DF"),
        _squad_row(6, "MID A", "MF"),
        _squad_row(7, "MID B", "MF"),
        _squad_row(8, "MID C", "MF"),
        _squad_row(9, "FWD A", "FW"),
        _squad_row(10, "FWD B", "FW"),
        _squad_row(11, "FWD C", "FW"),
        _squad_row(12, "DEF E", "DF"),
    ]
    ratings = _team_ratings(
        *[
            _rating(i, avg_rating=6.0 + i * 0.1, minutes_share=0.05 + i * 0.01)
            for i in range(1, 13)
        ]
    )
    id_map = [_map_row(i, i) for i in range(1, 13)]

    result = build_projected_xi(ratings, squad, id_map, formation=(1, 4, 3, 3))

    assert result["status"] == "ok"
    assert len(result["projected_xi"]) == 11
    assert len(result["bench"]) == 1
    assert result["bench"][0]["position"] == "DF"
    assert result["bench"][0]["squad_no"] == 2
    positions = [row["position"] for row in result["projected_xi"]]
    assert positions.count("GK") == 1
    assert positions.count("DF") == 4
    assert positions.count("MF") == 3
    assert positions.count("FW") == 3


def test_build_projected_xi_selects_by_minutes_share_not_rating():
    squad = [
        _squad_row(1, "KEEPER A", "GK"),
        _squad_row(2, "DEF HIGH RATING", "DF"),
        _squad_row(3, "DEF HIGH MINUTES", "DF"),
    ]
    ratings = _team_ratings(
        _rating(1, avg_rating=6.0, minutes_share=0.2),
        _rating(2, avg_rating=9.5, minutes_share=0.05),
        _rating(3, avg_rating=6.0, minutes_share=0.25),
    )
    id_map = [_map_row(1, 1), _map_row(2, 2), _map_row(3, 3)]

    result = build_projected_xi(ratings, squad, id_map, formation=(1, 1, 0, 0))

    assert result["status"] == "ok"
    assert len(result["projected_xi"]) == 2
    df_pick = next(row for row in result["projected_xi"] if row["position"] == "DF")
    assert df_pick["squad_no"] == 3
    assert df_pick["player_name"] == "DEF HIGH MINUTES"


def test_build_projected_xi_partial_when_bucket_thin():
    squad = [
        _squad_row(1, "KEEPER A", "GK"),
        _squad_row(2, "DEF A", "DF"),
        _squad_row(3, "MID A", "MF"),
    ]
    ratings = _team_ratings(
        _rating(1, avg_rating=7.0, minutes_share=0.1),
        _rating(2, avg_rating=6.5, minutes_share=0.2),
        _rating(3, avg_rating=6.8, minutes_share=0.15),
    )
    id_map = [_map_row(1, 1), _map_row(2, 2), _map_row(3, 3)]

    result = build_projected_xi(ratings, squad, id_map, formation=(1, 4, 3, 3))

    assert result["status"] == "partial"
    assert len(result["projected_xi"]) == 3
    assert {row["position"] for row in result["projected_xi"]} == {"GK", "DF", "MF"}


def test_build_projected_xi_no_qualifier_data_when_empty_ratings():
    squad = [_squad_row(1, "KEEPER A", "GK")]
    ratings = {"source": "sportmonks", "listed": [], "insufficient_data": []}

    result = build_projected_xi(ratings, squad, [_map_row(99, 1)])

    assert result["status"] == "no_qualifier_data"
    assert result["projected_xi"] == []
    assert result["bench"] == []


def test_build_projected_xi_manual_source_uses_squad_no():
    squad = [_squad_row(1, "CROCOMBE Max", "GK")]
    ratings = {
        "source": "manual",
        "listed": [
            {
                "player_id": 0,
                "player_name": "CROCOMBE Max",
                "avg_rating": 5.5,
                "matches_counted": 2,
                "squad_no": 1,
                "minutes_share": None,
            }
        ],
        "insufficient_data": [],
    }

    result = build_projected_xi(ratings, squad, [], formation=(1, 0, 0, 0))

    assert result["status"] == "ok"
    assert result["projected_xi"][0]["squad_no"] == 1
    assert result["bench"] == []


def test_build_projected_xi_excludes_unmapped_players():
    squad = [
        _squad_row(1, "KEEPER A", "GK"),
        _squad_row(2, "DEF A", "DF"),
    ]
    ratings = _team_ratings(
        _rating(1, avg_rating=7.0, minutes_share=0.1),
        _rating(999, avg_rating=8.0, minutes_share=0.9),
    )
    id_map = [_map_row(1, 1)]

    result = build_projected_xi(ratings, squad, id_map, formation=(1, 1, 0, 0))

    assert result["status"] == "partial"
    assert len(result["projected_xi"]) == 1
    assert result["projected_xi"][0]["squad_no"] == 1
    assert result["bench"] == []


def test_build_projected_xi_invalid_formation_raises():
    with pytest.raises(ValueError, match="formation must have"):
        build_projected_xi(
            _team_ratings(_rating(1, avg_rating=7.0, minutes_share=0.1)),
            [_squad_row(1, "A", "GK")],
            [_map_row(1, 1)],
            formation=(1, 4, 3),
        )


def test_build_projected_xi_manual_squad_no_ratings(tmp_path, monkeypatch):
    """Manual ratings keyed by squad_no should populate the XI without id_map."""
    squad = [
        _squad_row(10, "Star Player", "FW"),
        _squad_row(9, "Other Forward", "FW"),
    ]
    ratings = {
        "source": "manual",
        "listed": [
            {
                "player_id": 0,
                "squad_no": 10,
                "player_name": "Star Player",
                "avg_rating": 8.0,
                "matches_counted": 2,
                "minutes_share": None,
            },
            {
                "player_id": 0,
                "squad_no": 9,
                "player_name": "Other Forward",
                "avg_rating": 7.0,
                "matches_counted": 2,
                "minutes_share": None,
            },
        ],
        "insufficient_data": [],
    }
    overrides = tmp_path / "xi_overrides.json"
    overrides.write_text(
        json.dumps({"teams": {"TST": {"force_starters": [10]}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "src.player_ratings._XI_OVERRIDES_PATH",
        overrides,
    )
    result = build_projected_xi(
        ratings,
        squad,
        [],
        formation=(0, 0, 0, 2),
        team_code="TST",
    )
    assert result["status"] == "ok"
    assert [row["squad_no"] for row in result["projected_xi"]] == [10, 9]


def test_build_projected_xi_full_xi_override(tmp_path, monkeypatch):
    squad = [
        _squad_row(1, "Keeper", "GK"),
        _squad_row(4, "Defender", "DF"),
        _squad_row(8, "Midfielder", "MF"),
        _squad_row(9, "Forward", "FW"),
    ]
    overrides = tmp_path / "xi_overrides.json"
    overrides.write_text(
        json.dumps({"teams": {"TST": {"full_xi": [1, 4, 8, 9]}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr("src.player_ratings._XI_OVERRIDES_PATH", overrides)
    result = build_projected_xi(
        {"source": "manual", "listed": [], "insufficient_data": []},
        squad,
        [],
        team_code="TST",
    )
    assert [row["squad_no"] for row in result["projected_xi"]] == [1, 4, 8, 9]


def test_flatten_projected_lineup_rows_for_neon():
    display = {
        "status": "ok",
        "ratings_source": "sportmonks",
        "projected_xi": [
            {
                "squad_no": 10,
                "player_name": "Messi",
                "position": "FW",
                "avg_rating": 7.8,
                "minutes_share": 0.9,
                "matches_counted": 5,
                "match_method": "dob+name",
            }
        ],
        "bench": [
            {
                "squad_no": 7,
                "player_name": "Bench Player",
                "position": "MF",
                "avg_rating": 6.5,
                "minutes_share": 0.2,
                "matches_counted": 3,
                "match_method": "dob+name",
            }
        ],
    }
    snap = date(2026, 6, 4)
    rows = _flatten_projected_lineup_rows("ARG", display, snapshot_date=snap)
    assert len(rows) == 2
    assert rows[0]["team_code"] == "ARG"
    assert rows[0]["lineup_role"] == "projected_xi"
    assert rows[0]["lineup_slot"] == 1
    assert rows[0]["snapshot_date"] == snap
    assert rows[0]["team_xi_status"] == "ok"
    assert rows[1]["lineup_role"] == "bench"
    assert rows[1]["lineup_slot"] == 1
