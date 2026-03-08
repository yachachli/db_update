"""Tests for ats_service with mock data."""
import pytest
import pandas as pd
from unittest.mock import patch
from app.services.ats_service import (
    _net_tier,
    _record_from_bools,
    _empty_ats_full_structure,
    get_ats_by_team,
)


def test_net_tier():
    assert _net_tier(5) == 1
    assert _net_tier(10) == 1
    assert _net_tier(15) == 2
    assert _net_tier(30) == 3
    assert _net_tier(60) == 4
    assert _net_tier(100) == 5


def test_record_from_bools():
    r = _record_from_bools(6, 10)
    assert r["record"] == "6-4"
    assert r["cover_pct"] == 60.0
    r0 = _record_from_bools(0, 0)
    assert r0["record"] == "0-0"
    assert r0["cover_pct"] == 0.0


def test_empty_ats_structure():
    s = _empty_ats_full_structure("Duke")
    assert s["team"] == "Duke"
    assert "overall" in s
    assert "vs_vegas" in s["overall"]
    assert "by_net_tier" in s
    assert len(s["by_net_tier"]) == 5


@patch("app.services.ats_service._load_ats_complete", return_value=None)
def test_get_ats_by_team_fanmatch_fallback(mock_load):
    out = get_ats_by_team("NonexistentTeamXYZ")
    assert out["team"] == "NonexistentTeamXYZ"
    assert "overall" in out
    assert "vs_kenpom" in out["overall"]
