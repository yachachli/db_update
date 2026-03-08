"""
Pydantic models for API responses and internal data.
Task 6: TeamProfile and related schemas.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field


# --- Team Profile (Task 6) ---


class ATSByTier(BaseModel):
    """ATS performance for one NET tier."""

    tier: str
    record: str
    cover_pct: float
    avg_margin_vs_spread: Optional[float] = None


class TeamProfile(BaseModel):
    """Unified team profile from KenPom, four factors, height, ATS, odds."""

    name: str
    conference: str = ""
    kenpom_rank: int = 0

    # Efficiency
    adj_oe: float = 0.0
    adj_de: float = 0.0
    adj_em: float = 0.0
    adj_tempo: float = 0.0

    # Four Factors (Offense)
    off_efg: float = 0.0
    off_to: float = 0.0
    off_or: float = 0.0
    off_ft_rate: float = 0.0

    # Four Factors (Defense)
    def_efg: float = 0.0
    def_to: float = 0.0
    def_or: float = 0.0
    def_ft_rate: float = 0.0

    # Shooting splits
    three_pt_pct: float = 0.0
    two_pt_pct: float = 0.0
    ft_pct: float = 0.0
    three_pt_rate: float = 0.0  # 3PA as % of FGA

    # Physical
    avg_height: float = 0.0
    experience: float = 0.0
    bench_minutes: float = 0.0

    # Recency (from prediction/team_service)
    recent_adj_oe: Optional[float] = None
    recent_adj_de: Optional[float] = None
    recent_record: Optional[str] = None
    recent_ats_record: Optional[str] = None
    recent_margin_vs_expected: Optional[float] = None
    trend_direction: Optional[str] = None  # "rising", "falling", "stable"

    # ATS by NET tier
    ats_by_tier: dict[str, Any] = Field(default_factory=dict)

    # Odds
    futures_odds: Optional[int] = None  # American
    implied_tournament_prob: Optional[float] = None

    class Config:
        extra = "allow"


# --- Matchup / Prediction (Task 4, 7) ---


class MatchupPredictionRequest(BaseModel):
    """POST /api/matchups/predict body."""

    team_a: str
    team_b: str
    neutral: bool = True
    weights: Optional[dict[str, float]] = None


class KeyFactor(BaseModel):
    """One key matchup factor."""

    factor: str
    team: str
    magnitude: str


class KenPomBaseline(BaseModel):
    """Pure KenPom prediction for comparison."""

    predicted_margin: float
    win_prob_a: float


class MatchupPredictionResponse(BaseModel):
    """Response from prediction model."""

    team_a: str
    team_b: str
    predicted_score_a: int
    predicted_score_b: int
    predicted_margin: float
    win_prob_a: float
    win_prob_b: float
    key_factors: list[KeyFactor] = Field(default_factory=list)
    weights_applied: dict[str, float] = Field(default_factory=dict)
    kenpom_baseline: Optional[KenPomBaseline] = None
