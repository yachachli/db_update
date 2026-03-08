"""
Task 7: Teams API — list teams, team profile, schedule, compare.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

import pandas as pd
from app.models.schemas import TeamProfile
from app.services.team_service import get_team_profile, list_teams, get_team_schedule_cached

router = APIRouter(prefix="/api/teams", tags=["teams"])


@router.get("", response_model=list)
def get_teams(season: Optional[str] = Query(None, description="Season year (e.g. 2026)")):
    """List all teams with basic stats (name, rank, conference, adj_em)."""
    return list_teams(season=season)


@router.get("/compare")
def compare_teams(
    a: str = Query(..., description="Team A name"),
    b: str = Query(..., description="Team B name"),
    season: Optional[str] = Query(None),
):
    """Side-by-side comparison of two teams."""
    pa = get_team_profile(a, season=season)
    pb = get_team_profile(b, season=season)
    if pa is None:
        raise HTTPException(status_code=404, detail=f"Team not found: {a}")
    if pb is None:
        raise HTTPException(status_code=404, detail=f"Team not found: {b}")
    return {"team_a": pa.model_dump(), "team_b": pb.model_dump()}


@router.get("/{team_name}", response_model=TeamProfile)
def get_team(team_name: str, season: Optional[str] = Query(None)):
    """Full team profile by name."""
    profile = get_team_profile(team_name, season=season)
    if profile is None:
        raise HTTPException(status_code=404, detail=f"Team not found: {team_name}")
    return profile


@router.get("/{team_name}/schedule")
def get_team_schedule(team_name: str, season: Optional[str] = Query(None)):
    """Season schedule with results (reconstructed from FanMatch)."""
    schedule = get_team_schedule_cached(team_name, season=season)
    if schedule is None or len(schedule) == 0:
        return {"team": team_name, "schedule": [], "message": "No schedule found for this team."}
    return {"team": team_name, "schedule": schedule.to_dict(orient="records")}


@router.get("/{team_name}/trend")
def get_team_trend(team_name: str, n: int = Query(10, ge=1, le=30, description="Last N games")):
    """Game-by-game trend: margin_vs_expected, rolling 5-game average. Visualization-ready."""
    from app.services.schedule_service import get_team_schedule
    schedule = get_team_schedule(team_name)
    if schedule is None or len(schedule) == 0:
        return {"team": team_name, "games": [], "rolling_5_avg": None, "message": "No schedule data."}
    df = schedule.tail(n).copy()
    if "predicted_margin_team" not in df.columns:
        df["predicted_margin_team"] = df.get("predicted_margin")
    def mve(r):
        am, pm = r.get("actual_margin"), r.get("predicted_margin_team")
        if am is None or pd.isna(am) or pm is None or pd.isna(pm):
            return None
        return float(am) - float(pm)
    df["margin_vs_expected"] = df.apply(mve, axis=1)
    games = []
    for _, row in df.iterrows():
        games.append({
            "date": row["date"],
            "opponent": row["opponent"],
            "location": row["location"],
            "predicted_margin": row.get("predicted_margin_team"),
            "actual_margin": row.get("actual_margin"),
            "margin_vs_expected": row.get("margin_vs_expected"),
        })
    mve = df["margin_vs_expected"].dropna()
    rolling_5 = mve.rolling(5, min_periods=1).mean().tolist() if len(mve) else None
    return {"team": team_name, "games": games, "rolling_5_avg": round(float(mve.tail(5).mean()), 1) if len(mve) >= 1 else None}
