"""Tests for recency_service. Mock schedule and ratings."""
import sys
from pathlib import Path

# Allow running from project root: python backend/tests/test_recency_service.py
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))

import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.services.recency_service import calculate_recency_metrics


def _mock_ratings():
    return pd.DataFrame([
        {"Team": "Duke", "AdjO": 115.0, "AdjD": 95.0, "AdjT": 68.0},
        {"Team": "North Carolina", "AdjO": 112.0, "AdjD": 98.0, "AdjT": 67.0},
        {"Team": "Virginia", "AdjO": 105.0, "AdjD": 92.0, "AdjT": 62.0},
    ])


def test_recency_trend_rising():
    today = datetime.now(timezone.utc).date()
    schedule = pd.DataFrame([
        {"date": (today - timedelta(days=i)).isoformat(), "opponent": "Virginia", "predicted_margin_team": 5.0, "actual_margin": 5.0 + 3.0, "covered_prediction": True}
        for i in range(5)
    ])
    out = calculate_recency_metrics("Duke", schedule, _mock_ratings(), window_days=21)
    assert out is not None
    assert out["trend_direction"] == "rising"
    assert out["recent_margin_vs_expected"] == 3.0
    assert out["games_in_window"] == 5


def test_recency_trend_falling():
    today = datetime.now(timezone.utc).date()
    schedule = pd.DataFrame([
        {"date": (today - timedelta(days=i)).isoformat(), "opponent": "Virginia", "predicted_margin_team": 5.0, "actual_margin": 1.0, "covered_prediction": False}
        for i in range(5)
    ])
    out = calculate_recency_metrics("Duke", schedule, _mock_ratings(), window_days=21)
    assert out is not None
    assert out["trend_direction"] == "falling"


def test_recency_empty_schedule():
    out = calculate_recency_metrics("Duke", pd.DataFrame(), _mock_ratings())
    assert out is None


def test_recency_zero_games_in_window():
    today = datetime.now(timezone.utc).date()
    schedule = pd.DataFrame([
        {"date": (today - timedelta(days=60)).isoformat(), "opponent": "Virginia", "predicted_margin_team": 5.0, "actual_margin": 8.0, "covered_prediction": True}
    ])
    out = calculate_recency_metrics("Duke", schedule, _mock_ratings(), window_days=21)
    assert out is None
