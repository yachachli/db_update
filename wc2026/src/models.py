"""Domain data models for the World Cup 2026 predictive model.

Defines the typed structures passed between pipeline stages: teams,
matches, attack/defense ratings, and the final outcome-probability
(win/draw/loss) result objects.

Models are frozen (immutable) by default. The only exception is
``TeamRating``, whose normalized and final scores are filled in during
later pipeline stages and therefore needs to support mutation.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import datetime

__all__ = [
    "Team",
    "MatchStats",
    "TeamRating",
    "TournamentBaseline",
    "MatchPrediction",
]


@dataclass(frozen=True, slots=True)
class Team:
    """A national team and its FIFA-ranking metadata."""

    team_id: int
    name: str
    confederation: str  # UEFA, CONMEBOL, CONCACAF, AFC, CAF, OFC
    fifa_points: float
    fifa_rank: int
    is_host: bool


@dataclass(frozen=True, slots=True)
class MatchStats:
    """One team's performance in a single match.

    Holds both the offensive stats (what the team created) and the
    defensive stats (what it conceded), recorded from the perspective of
    ``team_id`` against ``opponent_id``.
    """

    match_id: int
    date: datetime
    team_id: int
    opponent_id: int
    opponent_fifa_points: float  # at the time of the match
    competition_type: str
    venue: str  # home, away, neutral

    # Offensive (created)
    goals_scored: int
    xg_created: float
    big_chances_created: int
    shots_on_target: int
    xgot_created: float

    # Defensive (conceded)
    goals_conceded: int
    xg_conceded: float
    big_chances_conceded: int
    shots_on_target_conceded: int
    xgot_conceded: float

    # Display-only fields (NOT used in any rating calculation). These have
    # defaults so existing positional construction stays valid.
    possession_pct: float = 0.0
    opponent_name: str = ""

    @property
    def outcome(self) -> str:
        """Return "W", "L", or "D" from this team's perspective."""
        if self.goals_scored > self.goals_conceded:
            return "W"
        if self.goals_scored < self.goals_conceded:
            return "L"
        return "D"

    @property
    def scoreline_str(self) -> str:
        """Return a formatted scoreline like "2-1 vs Israel (H)"."""
        venue_marker = {"home": "H", "away": "A", "neutral": "N"}[self.venue]
        return (
            f"{self.goals_scored}-{self.goals_conceded} "
            f"vs {self.opponent_name} ({venue_marker})"
        )


@dataclass(slots=True)
class TeamRating:
    """Computed attack/defense rating for a single team.

    Not frozen: the raw scores are computed first, then normalized and
    finalized in later pipeline stages, so the object is mutated in place.
    """

    team_id: int
    attack_raw: float
    defense_raw: float
    attack_normalized: float
    defense_normalized: float
    attack_final: float
    defense_final: float
    matches_used: int  # flag teams with fewer than 5 matches as low-confidence

    def __post_init__(self) -> None:
        if self.matches_used < 5:
            warnings.warn(
                f"TeamRating for team_id={self.team_id} uses only "
                f"{self.matches_used} match(es) (< 5); rating may be "
                f"unreliable.",
                stacklevel=2,
            )


@dataclass(frozen=True, slots=True)
class TournamentBaseline:
    """Tournament-wide scoring baselines used to normalize ratings."""

    baseline_goals_per_match: float
    baseline_goals_per_team: float
    filtered_match_count: int
    fifa_points_threshold: float
    # How many matches were dropped for having all-zero core stats (a
    # data-quality signal, not a real goalless game). Defaults keep existing
    # constructions valid.
    excluded_zero_stat_matches: int = 0


@dataclass(frozen=True, slots=True)
class MatchPrediction:
    """Predicted outcome distribution for a single fixture.

    ``xg_a`` / ``xg_b`` are the dampened expected goals that actually drove the
    prediction; ``xg_a_raw`` / ``xg_b_raw`` are the pre-dampening values kept
    for transparency. For moderate matchups the two are nearly identical.
    """

    team_a_id: int
    team_b_id: int
    xg_a: float
    xg_b: float
    prob_a_win: float
    prob_draw: float
    prob_b_win: float
    scoreline_matrix: list[list[float]]  # 7x7 grid: 0-0 through 6-6
    most_likely_scoreline: tuple[int, int]

    # Pre-dampening expected goals (for transparency/debugging). Defaults keep
    # existing positional constructions valid.
    xg_a_raw: float = 0.0
    xg_b_raw: float = 0.0
