"""
Matchup analysis: today's FanMatch, historical FanMatch, run BracketIQ prediction.
"""

from datetime import datetime
from typing import Optional

import pandas as pd

from app.config import get_cache_dir, get_historical_dir, settings
from app.models.schemas import MatchupPredictionRequest, MatchupPredictionResponse
from app.models.prediction import BracketIQModel
from app.services.team_service import get_team_profile


def get_today_fanmatch(browser=None) -> Optional[list[dict]]:
    """Today's FanMatch predictions. If browser provided, scrape; else try cache."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache_dir = get_cache_dir()
    # Cache key for "today" would be fanmatch_YYYY-MM-DD; we don't cache by date in refresh_all, so we need to scrape if no historical file for today
    historical_dir = get_historical_dir()
    fm_path = historical_dir / "fanmatch_2026.parquet"
    if fm_path.exists():
        df = pd.read_parquet(fm_path)
        if "fanmatch_date" in df.columns:
            day = df[df["fanmatch_date"] == today]
            if not day.empty:
                return day.to_dict(orient="records")
    if browser is not None:
        from app.scrapers.kenpom_scraper import get_fanmatch_for_date
        fm = get_fanmatch_for_date(browser, today)
        if fm is not None and fm.fm_df is not None:
            return fm.fm_df.to_dict(orient="records")
    return None


def get_historical_fanmatch(date: str) -> Optional[list[dict]]:
    """Historical FanMatch for a given date (YYYY-MM-DD) from consolidated parquet."""
    historical_dir = get_historical_dir()
    fm_path = historical_dir / "fanmatch_2026.parquet"
    if not fm_path.exists():
        return None
    df = pd.read_parquet(fm_path)
    if "fanmatch_date" not in df.columns:
        return None
    day = df[df["fanmatch_date"] == date]
    if day.empty:
        return None
    return day.to_dict(orient="records")


def run_prediction(
    team_a: str,
    team_b: str,
    neutral: bool = True,
    weights: Optional[dict] = None,
    season: Optional[str] = None,
) -> MatchupPredictionResponse:
    """Run BracketIQ model for team_a vs team_b. Returns prediction response."""
    profile_a = get_team_profile(team_a, season=season)
    profile_b = get_team_profile(team_b, season=season)
    if profile_a is None:
        raise ValueError(f"Team not found: {team_a}")
    if profile_b is None:
        raise ValueError(f"Team not found: {team_b}")
    model = BracketIQModel(weights=weights)
    return model.predict_matchup(profile_a, profile_b, neutral=neutral)
