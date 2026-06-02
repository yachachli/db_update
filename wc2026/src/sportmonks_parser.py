"""Pure transformation from raw SportMonks JSON to our domain objects.

No API calls, no I/O. Given a raw fixture dict (as returned by
``SportmonksClient`` with the ``xGFixture`` include), produces a
:class:`~src.models.MatchStats` from a chosen team's perspective.

Stat extraction relies on the per-team statistics carried in the (lowercase)
``xgfixture`` array of the response -- despite its name, that include returns
the full statistics collection, not just xG (confirmed in
``scripts/resolve_xg_types.py``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.models import MatchStats
from src.sportmonks_client import WC_FINALS_LEAGUE_ID

__all__ = [
    "STAT_TYPE_IDS",
    "parse_fixture_to_match_stats",
    "extract_stat_value",
]

# SportMonks stat type_ids -> the stats we read. Resolved and verified in
# scripts/resolve_xg_types.py against a real qualifier fixture.
STAT_TYPE_IDS = {
    "goals": 52,
    "xg": 5304,
    "xgot": 5305,
    "big_chances_created": 580,
    "shots_on_target": 86,
    "xg_against": 9687,  # direct xGA; preferred primary source for xg_conceded
    "possession": 45,  # Ball Possession % (display-only)
}

# WC Qualification Intercontinental Playoffs (see leagues_catalog.json).
_WC_PLAYOFF_LEAGUE_ID = 729

# Kickoff timestamp format used by SportMonks (e.g. "2025-06-06 18:45:00").
_STARTING_AT_FORMAT = "%Y-%m-%d %H:%M:%S"


def extract_stat_value(
    fixture: dict[str, Any],
    participant_id: int,
    type_id: int,
    default: float = 0.0,
) -> float:
    """Return the value of one stat for one participant from ``xgfixture``.

    Scans ``fixture["xgfixture"]`` for the entry matching both
    ``participant_id`` and ``type_id`` and returns ``entry["data"]["value"]``.
    Returns ``default`` when the stat is absent.
    """
    entries = fixture.get("xgfixture")
    if not isinstance(entries, list):
        return default
    for entry in entries:
        if entry.get("participant_id") == participant_id and entry.get("type_id") == type_id:
            data = entry.get("data")
            if isinstance(data, dict) and data.get("value") is not None:
                return float(data["value"])
            return default
    return default


def _determine_venue(fixture: dict[str, Any], team_id: int) -> str:
    """Return "home", "away", or "neutral" for ``team_id`` in this fixture.

    World Cup finals matches are always neutral. Otherwise we honor an
    explicit neutral indicator on the fixture's venue, then fall back to the
    team's home/away location among the participants.
    """
    if fixture.get("league_id") == WC_FINALS_LEAGUE_ID:
        return "neutral"

    # Honor an explicit neutral flag if the API provides one (defensive: the
    # field's exact shape varies, so we check a few plausible locations).
    if _looks_neutral(fixture.get("venue")) or _looks_neutral(fixture):
        return "neutral"

    location = _team_location(fixture, team_id)
    if location in ("home", "away"):
        return location
    return "neutral"


def _looks_neutral(obj: Any) -> bool:
    """True if ``obj`` is a mapping carrying a truthy 'neutral' indicator."""
    if not isinstance(obj, dict):
        return False
    for key, value in obj.items():
        if "neutral" in str(key).lower() and bool(value):
            return True
    return False


def _team_location(fixture: dict[str, Any], team_id: int) -> str | None:
    """Return the participant's meta location ("home"/"away") for ``team_id``."""
    for participant in fixture.get("participants", []):
        if participant.get("id") == team_id:
            meta = participant.get("meta")
            if isinstance(meta, dict):
                return meta.get("location")
    return None


def _determine_competition_type(fixture: dict[str, Any]) -> str:
    """Map the fixture's league to a coarse competition type.

    Uses ``league_id`` (the league name is not included in the response):
    finals -> "wc_finals", intercontinental playoffs -> "wc_playoff",
    everything else in our league universe -> "wc_qualifier".
    """
    league_id = fixture.get("league_id")
    if league_id == WC_FINALS_LEAGUE_ID:
        return "wc_finals"
    if league_id == _WC_PLAYOFF_LEAGUE_ID:
        return "wc_playoff"
    return "wc_qualifier"


def _identify_opponent(
    fixture: dict[str, Any], team_id: int
) -> tuple[int, str]:
    """Return (opponent_id, opponent_name) for the non-``team_id`` participant."""
    for participant in fixture.get("participants", []):
        if participant.get("id") != team_id:
            return int(participant.get("id", 0)), str(participant.get("name", ""))
    return 0, ""


def parse_fixture_to_match_stats(
    fixture: dict[str, Any],
    team_id: int,
    opponent_fifa_points: float,
) -> MatchStats:
    """Transform one raw fixture into a MatchStats from ``team_id``'s view.

    Offensive stats are read from ``team_id``'s own entries; defensive
    (conceded) stats mirror the opponent's offensive entries. For
    ``xg_conceded`` we prefer this team's direct Expected Goals Against (xGA)
    value when present, falling back to the opponent's xG.
    """
    match_id = int(fixture["id"])
    date = datetime.strptime(fixture["starting_at"], _STARTING_AT_FORMAT)

    opponent_id, opponent_name = _identify_opponent(fixture, team_id)
    venue = _determine_venue(fixture, team_id)
    competition_type = _determine_competition_type(fixture)

    # Offensive: this team's own production.
    goals_scored = int(extract_stat_value(fixture, team_id, STAT_TYPE_IDS["goals"]))
    xg_created = extract_stat_value(fixture, team_id, STAT_TYPE_IDS["xg"])
    xgot_created = extract_stat_value(fixture, team_id, STAT_TYPE_IDS["xgot"])
    big_chances_created = int(
        extract_stat_value(fixture, team_id, STAT_TYPE_IDS["big_chances_created"])
    )
    shots_on_target = int(
        extract_stat_value(fixture, team_id, STAT_TYPE_IDS["shots_on_target"])
    )

    # Defensive: mirror the opponent's offensive production.
    goals_conceded = int(
        extract_stat_value(fixture, opponent_id, STAT_TYPE_IDS["goals"])
    )
    opponent_xg = extract_stat_value(fixture, opponent_id, STAT_TYPE_IDS["xg"])
    # Prefer this team's own xGA; fall back to the opponent's xG when absent.
    xg_conceded = extract_stat_value(
        fixture, team_id, STAT_TYPE_IDS["xg_against"], default=opponent_xg
    )
    xgot_conceded = extract_stat_value(fixture, opponent_id, STAT_TYPE_IDS["xgot"])
    big_chances_conceded = int(
        extract_stat_value(fixture, opponent_id, STAT_TYPE_IDS["big_chances_created"])
    )
    shots_on_target_conceded = int(
        extract_stat_value(fixture, opponent_id, STAT_TYPE_IDS["shots_on_target"])
    )

    # Display-only.
    possession_pct = extract_stat_value(
        fixture, team_id, STAT_TYPE_IDS["possession"], default=50.0
    )

    return MatchStats(
        match_id=match_id,
        date=date,
        team_id=team_id,
        opponent_id=opponent_id,
        opponent_fifa_points=opponent_fifa_points,
        competition_type=competition_type,
        venue=venue,
        goals_scored=goals_scored,
        xg_created=xg_created,
        big_chances_created=big_chances_created,
        shots_on_target=shots_on_target,
        xgot_created=xgot_created,
        goals_conceded=goals_conceded,
        xg_conceded=xg_conceded,
        big_chances_conceded=big_chances_conceded,
        shots_on_target_conceded=shots_on_target_conceded,
        xgot_conceded=xgot_conceded,
        possession_pct=possession_pct,
        opponent_name=opponent_name,
    )
