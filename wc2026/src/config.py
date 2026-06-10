"""Configuration and constants for the World Cup 2026 predictive model.

Loads environment variables (API credentials, base URLs) and exposes
tunable model parameters such as the match-window size, recency decay,
Dixon-Coles low-score correction, and host/confederation adjustments.

All tunables are module-level constants (the idiomatic Python choice for a
flat namespace of read-only settings). Mapping constants are wrapped in
``MappingProxyType`` so they cannot be mutated at runtime, and the offensive
and defensive stat-weight dictionaries are validated to sum to 1.0 on import.
"""

from __future__ import annotations

import logging
import os
from types import MappingProxyType
from typing import Final, Mapping

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SportMonks API credentials
# ---------------------------------------------------------------------------
SPORTMONKS_API_KEY: Final[str | None] = os.getenv("SPORTMONKS_API_KEY")
SPORTMONKS_BASE_URL: Final[str] = os.getenv(
    "SPORTMONKS_BASE_URL", "https://api.sportmonks.com/v3/football"
)

# ---------------------------------------------------------------------------
# Neon Postgres
# ---------------------------------------------------------------------------
# Accept several env var names: worldcup2026-predictor uses NEON_DATABASE_URL;
# db_update repo secrets typically use DB_URL or DATABASE_URL.
NEON_DATABASE_URL: Final[str | None] = (
    os.getenv("NEON_DATABASE_URL")
    or os.getenv("DB_URL")
    or os.getenv("DATABASE_URL")
)
if not NEON_DATABASE_URL:
    logger.warning(
        "NEON_DATABASE_URL (or DB_URL / DATABASE_URL) is not set. Database sync "
        "and cron jobs will be unavailable; local model operations that do not "
        "touch Neon still work."
    )

# ---------------------------------------------------------------------------
# Venue multipliers (match-quality weighting)
# ---------------------------------------------------------------------------
# A result earned away from home reflects more strongly on a team's quality
# than the same result at home, so away matches are weighted up and home
# matches down. Neutral venues (as at the World Cup itself) are the baseline.
VENUE_MULT_AWAY: Final[float] = 1.15
VENUE_MULT_NEUTRAL: Final[float] = 1.00
VENUE_MULT_HOME: Final[float] = 0.85

# ---------------------------------------------------------------------------
# Host adjustment
# ---------------------------------------------------------------------------
# Applied to both attack and defense ratings for the host nations.
HOST_BONUS: Final[float] = 1.07
HOST_TEAM_NAMES: Final[frozenset[str]] = frozenset({"USA", "Canada", "Mexico"})

# ---------------------------------------------------------------------------
# Confederation multipliers
# ---------------------------------------------------------------------------
CONFEDERATION_MULTIPLIERS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "UEFA": 1.05,
        "CONMEBOL": 1.05,
        "CONCACAF": 1.00,
        "AFC": 0.97,
        "CAF": 0.97,
        "OFC": 0.95,
    }
)

# ---------------------------------------------------------------------------
# Offensive / defensive stat weights (each must sum to 1.0)
# ---------------------------------------------------------------------------
OFFENSIVE_STAT_WEIGHTS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "xg_created": 0.40,
        "goals_scored": 0.20,
        "big_chances_created": 0.20,
        "shots_on_target": 0.10,
        "xgot_created": 0.10,
    }
)

DEFENSIVE_STAT_WEIGHTS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "xg_conceded": 0.40,
        "goals_conceded": 0.20,
        "big_chances_conceded": 0.20,
        "shots_on_target_conceded": 0.10,
        "xgot_conceded": 0.10,
    }
)

# Conversion factors to bring count-based stats onto the same goal-equivalent
# scale as xG and goals. Derived from empirical conversion rates in
# international football: ~1 in 3 big chances becomes a goal; ~1 in 3 shots
# on target becomes a goal. Applied before the stat weights so attack_raw /
# defense_raw end up in genuine goal units. These are tunable.
STAT_GOAL_CONVERSION_FACTORS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "xg_created": 1.0,  # already in goal units
        "goals_scored": 1.0,  # already in goal units
        "big_chances_created": 0.33,  # divide by ~3
        "shots_on_target": 0.33,  # divide by ~3
        "xgot_created": 1.0,  # already in goal units
        # Defensive mirrors
        "xg_conceded": 1.0,
        "goals_conceded": 1.0,
        "big_chances_conceded": 0.33,
        "shots_on_target_conceded": 0.33,
        "xgot_conceded": 1.0,
    }
)

# ---------------------------------------------------------------------------
# FIFA ranking / strength of schedule
# ---------------------------------------------------------------------------
REFERENCE_FIFA_POINTS: Final[int] = 1475  # median qualifier baseline
FIFA_STRENGTH_EXPONENT: Final[float] = 0.5  # square-root compression
# Opponents this many points below a team are excluded from baseline calc.
FIFA_POINTS_FILTER_THRESHOLD: Final[int] = 600

# FIFA prior blended into form-based ratings (0 = pure recent form, 1 = pure FIFA).
FIFA_FORM_BLEND_WEIGHT: Final[float] = 0.48
# Maps FIFA points gap to attack/defense priors before confederation/host scaling.
FIFA_PRIOR_EXPONENT: Final[float] = 0.72

# Qualifier windows with little xG data (goals-only fallback) are unreliable.
GOALS_ONLY_MATCH_THRESHOLD: Final[float] = 0.6  # fraction of last-5 window
GOALS_ONLY_FIFA_BLEND: Final[float] = 0.62  # extra pull toward FIFA when thin xG
# Hot streaks cannot exceed FIFA-implied attack by more than this ratio.
FORM_ATTACK_FIFA_CAP_RATIO: Final[float] = 1.05
# Goals-only defenses look artificially stingy; floor vulnerability at FIFA prior.
GOALS_ONLY_DEFENSE_FLOOR_RATIO: Final[float] = 1.0

# Baseline data-quality safety net. If the filtered match pool is too small or
# the computed per-team goal baseline is implausibly low (a symptom of
# zero-stat / missing-data matches polluting the pool), fall back to a
# realistic hardcoded value derived from historical FIFA tournament data.
BASELINE_GOALS_FLOOR: Final[float] = 1.35  # realistic per-team goals/match
BASELINE_MIN_MATCH_COUNT: Final[int] = 50  # min pool size for a confident baseline

# ---------------------------------------------------------------------------
# Match window
# ---------------------------------------------------------------------------
MATCH_WINDOW_SIZE: Final[int] = 5  # last 5 competitive matches
# Display-only player ratings use the same recency window. Once WC finals
# fixtures are played they enter this window automatically (league 732 is included
# in Sportmonks fixture fetches) and averages repopulate on each report build.
PLAYER_RATINGS_MATCH_WINDOW: Final[int] = MATCH_WINDOW_SIZE

# ---------------------------------------------------------------------------
# Poisson / Dixon-Coles
# ---------------------------------------------------------------------------
DIXON_COLES_TAU: Final[float] = -0.05
MAX_GOALS_FOR_MATRIX: Final[int] = 6  # 0-0 through 6-6 scoreline matrix

# ---------------------------------------------------------------------------
# Most-likely scoreline (display heuristic)
# ---------------------------------------------------------------------------
# Win/draw/loss probabilities still come from the Dixon-Coles Poisson matrix.
# The headline scoreline is derived from rounded xG; when the gap exceeds this
# threshold the stronger side is shown winning by exactly one goal.
SCORELINE_XG_GAP_THRESHOLD: Final[float] = 0.5
# Sub-1.0 xG must reach this level before rounding up to one goal (0.68 stays 0).
SCORELINE_ZERO_BUCKET_ROUND_THRESHOLD: Final[float] = 0.75
# For xG in [1, 2), [2, 3), ... round up only when strictly above n + this
# fraction (e.g. 1.51 -> 2, 1.50 -> 1).
SCORELINE_UPPER_BUCKET_ROUND_FRACTION: Final[float] = 0.5

# ---------------------------------------------------------------------------
# xG dampening (prediction-time calibration)
# ---------------------------------------------------------------------------
# The attack x defense multiplicative model can produce unrealistic xG for
# extreme mismatches. These compress predicted xG toward realistic ranges
# immediately before it enters the Poisson distribution (see math_utils.dampen_xg).
XG_DAMPENING_ALPHA: Final[float] = 0.85
XG_HARD_CEILING: Final[float] = 4.5
# Notes:
# - alpha=0.85 compresses high xG modestly. xG of 5.0 -> ~3.8,
#   xG of 7.0 -> ~5.0 (but capped by ceiling), xG of 1.5 -> ~1.4.
# - ceiling=4.5 reflects the empirical observation that real World Cup
#   matches essentially never produce team-xG above 4.0. 4.5 as ceiling
#   leaves headroom for genuinely lopsided matchups without crossing
#   into implausible territory.
# - Both values are tunable. Backtest against historical tournaments to refine.

# ---------------------------------------------------------------------------
# Host dynamic blending (in-tournament)
# ---------------------------------------------------------------------------
# As host teams play World Cup matches, blend their pre-tournament rating
# toward in-tournament form: weight on tournament data after each match.
HOST_BLEND_AFTER_MATCH_1: Final[float] = 0.30
HOST_BLEND_AFTER_MATCH_2: Final[float] = 0.50
HOST_BLEND_AFTER_MATCH_3: Final[float] = 1.00

# ---------------------------------------------------------------------------
# Import-time validation
# ---------------------------------------------------------------------------
_WEIGHT_SUM_TOLERANCE: Final[float] = 1e-9


def _validate_weight_sums() -> None:
    """Ensure each stat-weight mapping sums to 1.0 (within tolerance)."""
    for name, weights in (
        ("OFFENSIVE_STAT_WEIGHTS", OFFENSIVE_STAT_WEIGHTS),
        ("DEFENSIVE_STAT_WEIGHTS", DEFENSIVE_STAT_WEIGHTS),
    ):
        total = sum(weights.values())
        if abs(total - 1.0) > _WEIGHT_SUM_TOLERANCE:
            raise ValueError(
                f"{name} must sum to 1.0, got {total!r} "
                f"(diff {total - 1.0:+.3e})."
            )


_validate_weight_sums()
