"""Output assembly layer: produce the final matchup report as JSON.

Combines two teams' recent form, their computed ratings, and the match
prediction into a single JSON-serializable structure for downstream
consumers (file export, web frontend, etc.). JSON is the only output format.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from src.config import XG_DAMPENING_ALPHA, XG_HARD_CEILING
from src.models import MatchPrediction, MatchStats, Team, TeamRating

__all__ = [
    "MatchupReport",
    "build_matchup_report",
    "matchup_report_to_dict",
    "matchup_report_to_json",
]

logger = logging.getLogger(__name__)

_RECENT_MATCH_LIMIT = 5
_PROB_SUM_TOLERANCE = 0.001


_DATA_SOURCE_QUALIFIER = "qualifier_matches"
_DATA_SOURCE_HOST = "synthetic_host_override"
_SYNTHETIC_HOST_NOTE = (
    "Host nation — no WC qualifier data available. Rating based on hardcoded "
    "approximation pending first WC 2026 match."
)


@dataclass(frozen=True, slots=True)
class MatchupReport:
    """Assembled inputs and output for a single fixture's report."""

    team_a_name: str
    team_b_name: str

    # Last 5 matches for each team, most recent first.
    team_a_recent_matches: list[MatchStats]
    team_b_recent_matches: list[MatchStats]

    prediction: MatchPrediction

    # Rating context (for debugging and transparency).
    team_a_rating: TeamRating
    team_b_rating: TeamRating

    # Provenance: where each team's rating came from. Hosts use a hardcoded
    # synthetic override (see pipeline + data/host_team_overrides.json); all
    # other teams derive their rating from real qualifier matches.
    team_a_data_source: str = _DATA_SOURCE_QUALIFIER
    team_b_data_source: str = _DATA_SOURCE_QUALIFIER
    team_a_host_reasoning: str | None = None
    team_b_host_reasoning: str | None = None


def build_matchup_report(
    team_a: Team,
    team_b: Team,
    team_a_matches: list[MatchStats],
    team_b_matches: list[MatchStats],
    team_a_rating: TeamRating,
    team_b_rating: TeamRating,
    prediction: MatchPrediction,
    team_a_data_source: str = _DATA_SOURCE_QUALIFIER,
    team_b_data_source: str = _DATA_SOURCE_QUALIFIER,
    team_a_host_reasoning: str | None = None,
    team_b_host_reasoning: str | None = None,
) -> MatchupReport:
    """Assemble a MatchupReport, sorting matches most-recent-first.

    Each team's matches are sorted by date descending and truncated to the
    last five. ``*_data_source`` / ``*_host_reasoning`` carry rating provenance
    so the JSON output can explain why a host's rating was computed differently.
    """
    return MatchupReport(
        team_a_name=team_a.name,
        team_b_name=team_b.name,
        team_a_recent_matches=_recent_first(team_a_matches),
        team_b_recent_matches=_recent_first(team_b_matches),
        prediction=prediction,
        team_a_rating=team_a_rating,
        team_b_rating=team_b_rating,
        team_a_data_source=team_a_data_source,
        team_b_data_source=team_b_data_source,
        team_a_host_reasoning=team_a_host_reasoning,
        team_b_host_reasoning=team_b_host_reasoning,
    )


def _recent_first(matches: list[MatchStats]) -> list[MatchStats]:
    """Sort by date descending and keep only the most recent five."""
    ordered = sorted(matches, key=lambda m: m.date, reverse=True)
    return ordered[:_RECENT_MATCH_LIMIT]


def matchup_report_to_dict(report: MatchupReport) -> dict[str, Any]:
    """Build the JSON-serializable dict form of the report."""
    prediction = report.prediction

    prob_a = prediction.prob_a_win
    prob_draw = prediction.prob_draw
    prob_b = prediction.prob_b_win
    prob_total = prob_a + prob_draw + prob_b
    if abs(prob_total - 1.0) > _PROB_SUM_TOLERANCE:
        logger.warning(
            "Win probabilities sum to %.6f (expected ~1.0) for %s vs %s.",
            prob_total, report.team_a_name, report.team_b_name,
        )

    a_goals, b_goals = prediction.most_likely_scoreline

    return {
        "matchup": {
            "team_a": report.team_a_name,
            "team_b": report.team_b_name,
        },
        "recent_form": {
            "team_a": _recent_form_section(
                report.team_a_rating, report.team_a_recent_matches
            ),
            "team_b": _recent_form_section(
                report.team_b_rating, report.team_b_recent_matches
            ),
        },
        "prediction": {
            "expected_goals": {
                "team_a": round(prediction.xg_a, 2),
                "team_b": round(prediction.xg_b, 2),
                "team_a_raw": round(prediction.xg_a_raw, 2),
                "team_b_raw": round(prediction.xg_b_raw, 2),
                "dampening_alpha": XG_DAMPENING_ALPHA,
                "dampening_ceiling": XG_HARD_CEILING,
            },
            "win_probabilities": {
                "team_a_win": round(prob_a, 4),
                "draw": round(prob_draw, 4),
                "team_b_win": round(prob_b, 4),
            },
            "win_probabilities_pct": {
                "team_a_win": f"{prob_a:.1%}",
                "draw": f"{prob_draw:.1%}",
                "team_b_win": f"{prob_b:.1%}",
            },
            "most_likely_scoreline": {
                "team_a_goals": a_goals,
                "team_b_goals": b_goals,
                "as_string": f"{a_goals}-{b_goals}",
            },
        },
        "model_internals": {
            "team_a": _rating_to_dict(
                report.team_a_rating, report.team_a_data_source,
                report.team_a_host_reasoning,
            ),
            "team_b": _rating_to_dict(
                report.team_b_rating, report.team_b_data_source,
                report.team_b_host_reasoning,
            ),
        },
    }


def _recent_form_section(
    rating: TeamRating, matches: list[MatchStats]
) -> Any:
    """Return the recent-form payload for one team.

    A synthetic host rating (``matches_used == 0``) has no qualifier matches,
    so it returns a status/note object instead of a list.
    """
    if rating.matches_used == 0:
        return {
            "status": "synthetic_host_rating",
            "note": _SYNTHETIC_HOST_NOTE,
            "matches": [],
        }
    return [_match_to_dict(m) for m in matches]


def matchup_report_to_json(report: MatchupReport, indent: int = 2) -> str:
    """Serialize the report to a JSON string (UTF-8 friendly)."""
    return json.dumps(
        matchup_report_to_dict(report), indent=indent, ensure_ascii=False
    )


def _match_to_dict(match: MatchStats) -> dict[str, Any]:
    """Render one MatchStats entry for the recent-form list."""
    return {
        "date": match.date.strftime("%Y-%m-%d"),
        "opponent": match.opponent_name,
        "venue": match.venue,
        "result": match.outcome,
        "score": f"{match.goals_scored}-{match.goals_conceded}",
        "goals_scored": match.goals_scored,
        "goals_conceded": match.goals_conceded,
        "possession_pct": round(match.possession_pct, 2),
    }


def _rating_to_dict(
    rating: TeamRating,
    data_source: str = _DATA_SOURCE_QUALIFIER,
    host_reasoning: str | None = None,
) -> dict[str, Any]:
    """Render the rating internals exposed in the report.

    Includes a ``data_source`` provenance marker; host overrides additionally
    carry the ``host_override_reasoning`` string.
    """
    internals: dict[str, Any] = {
        "attack_final": round(rating.attack_final, 3),
        "defense_final": round(rating.defense_final, 3),
        "matches_used": rating.matches_used,
        "data_source": data_source,
    }
    if data_source == _DATA_SOURCE_HOST and host_reasoning:
        internals["host_override_reasoning"] = host_reasoning
    return internals
