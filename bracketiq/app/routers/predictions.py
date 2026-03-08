"""
Task 7 / Phase 1.5: Predictions API — model diagnostics, ATS, conference accuracy, model edge.
"""

from typing import Optional

from fastapi import APIRouter, Query

from app.services.ats_service import get_ats_performance_breakdown, get_conference_accuracy

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


@router.get("/model-diagnostics")
def model_diagnostics():
    """Our model's accuracy vs KenPom baseline. 2026 target: MAE margin 9.15, MAE totals 13.26."""
    # Placeholder until we have our model's backtest; report KenPom baseline
    return {
        "kenpom_baseline_2026": {
            "mae_margin": 9.15,
            "mae_totals": 13.26,
            "mae_margin_conf_only": 8.81,
            "bias_home": -0.19,
        },
        "our_model": "Run historical FanMatch collection and ATS pipeline to compute.",
    }


@router.get("/ats-performance")
def ats_performance(season: Optional[str] = Query(None)):
    """ATS performance breakdown (by tier, conference)."""
    return get_ats_performance_breakdown(season=season)


@router.get("/conference-accuracy")
def conference_accuracy(conference: str = Query(..., description="e.g. SEC, ACC"), season: Optional[str] = Query(None)):
    """Model accuracy by conference."""
    return get_conference_accuracy(conference, season=season)


@router.get("/model-edge")
def get_model_edge(min_edge: float = Query(2.0, description="Min margin difference from Vegas (pts)")):
    """Today's games where our predicted margin differs from Vegas spread by more than min_edge (high-conviction picks)."""
    from app.scrapers.odds_scraper import get_current_odds, calculate_consensus_line, odds_to_kenpom_name
    from app.services.matchup_service import run_prediction
    try:
        games_raw = get_current_odds()
    except Exception:
        return {"games": [], "message": "Could not fetch current odds."}
    edges = []
    for g in games_raw:
        home_odds = g.get("home_team", "")
        away_odds = g.get("away_team", "")
        home_kp = odds_to_kenpom_name(home_odds)
        away_kp = odds_to_kenpom_name(away_odds)
        line = calculate_consensus_line(g.get("bookmakers", []))
        if not line or line.get("spread") is None:
            continue
        vegas_spread = float(line["spread"])
        try:
            pred = run_prediction(home_kp, away_kp, neutral=False, weights=None)
        except Exception:
            continue
        our_margin = pred.predicted_margin
        edge = our_margin - vegas_spread
        if abs(edge) < min_edge:
            continue
        confidence = "high" if abs(edge) >= 4 else "medium"
        recency_factor = None
        if pred.weights_applied.get("recency"):
            recency_factor = f"Recency weight {pred.weights_applied.get('recency')}"
        edges.append({
            "game": f"{away_kp} @ {home_kp}",
            "our_margin": round(our_margin, 1),
            "vegas_spread": vegas_spread,
            "edge": round(edge, 1),
            "confidence": confidence,
            "recency_factor": recency_factor,
        })
    return {"games": edges, "min_edge_used": min_edge}
