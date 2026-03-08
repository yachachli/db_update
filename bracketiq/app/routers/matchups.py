"""
Task 7: Matchups API — predict, today's FanMatch, historical FanMatch.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Body

from app.models.schemas import MatchupPredictionRequest, MatchupPredictionResponse
from app.services.matchup_service import run_prediction, get_today_fanmatch, get_historical_fanmatch

router = APIRouter(prefix="/api/matchups", tags=["matchups"])


@router.post("/predict", response_model=MatchupPredictionResponse)
def predict_matchup(
    body: MatchupPredictionRequest = Body(...),
):
    """Run BracketIQ prediction for two teams."""
    try:
        return run_prediction(
            team_a=body.team_a,
            team_b=body.team_b,
            neutral=body.neutral,
            weights=body.weights,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/today")
def today_fanmatch():
    """Today's FanMatch predictions (from historical cache or scrape)."""
    data = get_today_fanmatch(browser=None)
    if data is None:
        return {"games": [], "message": "No FanMatch data for today. Run historical collector or provide browser."}
    return {"games": data}


@router.get("/history")
def history_fanmatch(date: str):
    """Historical FanMatch for a date (YYYY-MM-DD)."""
    data = get_historical_fanmatch(date)
    if data is None:
        return {"games": [], "message": "No data for this date."}
    return {"games": data}
