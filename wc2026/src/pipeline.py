"""End-to-end orchestration for the World Cup 2026 predictive model.

Wires every module together behind two operations:

* :func:`bootstrap_tournament_pool` -- an EAGER ROSTER BOOTSTRAP that, on first
  invocation, fetches all 48 teams' recent qualifier matches in one batch,
  resolves every team and opponent to FIFA points, builds a shared pool of
  ``MatchStats``, computes the tournament baseline once, and caches the lot.
* :func:`predict_matchup` -- runs instantly against the cached pool to produce
  a JSON matchup report.

This module only orchestrates; the math, aggregation, prediction, parsing,
FIFA and mapping logic all live in their own modules and are untouched here.
"""

from __future__ import annotations

import difflib
import json
import logging
import warnings
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src import config
from src.aggregation import compute_team_rating
from src.fifa_rankings import (
    FifaRankingRelease,
    FifaRankingsClient,
    get_current_rankings,
)
from src.math_utils import compute_baseline_goals
from src.models import MatchStats, Team, TeamRating, TournamentBaseline
from src.player_ratings import (
    build_matchup_player_display,
    empty_team_player_display,
)
from src.prediction import predict_match
from src.reporting import build_matchup_report, matchup_report_to_json
from src.sportmonks_client import SportmonksClient, SportmonksError
from src.sportmonks_parser import parse_fixture_to_match_stats
from src.team_mapping import TeamFifaMapper, TeamFifaMapping

__all__ = [
    "TournamentPool",
    "bootstrap_tournament_pool",
    "predict_matchup",
    "predict_matchup_by_id",
    "load_host_overrides",
]

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ROSTER_PATH = _PROJECT_ROOT / "data" / "wc2026_teams.json"
_DEFAULT_CACHE_PATH = Path("data/cache/tournament_pool.json")
_DEFAULT_HOST_OVERRIDES_PATH = Path("data/host_team_overrides.json")
_HOST_CODES = frozenset({"USA", "CAN", "MEX"})
_POOL_SCHEMA_VERSION = 2
_DATA_SOURCE_QUALIFIER = "qualifier_matches"
_DATA_SOURCE_HOST = "synthetic_host_override"


@dataclass(frozen=True, slots=True)
class TournamentPool:
    """Cached state of all data needed for predictions."""

    teams: dict[int, Team]                        # keyed by sportmonks_team_id
    matches_by_team: dict[int, list[MatchStats]]  # team_id -> their last 5
    all_matches: list[MatchStats]                 # flat pool for baseline
    baseline: TournamentBaseline
    fifa_release: FifaRankingRelease
    bootstrapped_at: datetime
    failed_teams: tuple[str, ...] = ()            # teams that could not bootstrap
    host_ratings: dict[int, TeamRating] = field(default_factory=dict)  # synthetic


# ===========================================================================
# Bootstrap
# ===========================================================================
def bootstrap_tournament_pool(
    force_refresh: bool = False,
    cache_path: Path = _DEFAULT_CACHE_PATH,
    cache_ttl_hours: int = 24,
) -> TournamentPool:
    """Build (or load) the full tournament pool. See module docstring."""
    cache_path = _absolute(cache_path)

    if not force_refresh:
        cached = _load_pool_if_fresh(cache_path, cache_ttl_hours)
        if cached is not None:
            logger.info(
                "Loaded tournament pool from cache (%d teams, bootstrapped %s).",
                len(cached.matches_by_team), cached.bootstrapped_at.isoformat(),
            )
            return cached

    logger.info("Bootstrapping tournament pool from scratch...")
    roster = _load_roster()
    host_overrides = load_host_overrides()
    fifa_release = get_current_rankings()
    logger.info("Using FIFA release %s (%s).",
                fifa_release.release_date, fifa_release.date_id)

    client = SportmonksClient()
    mapper = TeamFifaMapper(fifa_release)

    teams: dict[int, Team] = {}
    matches_by_team: dict[int, list[MatchStats]] = {}
    failed_teams: list[str] = []
    opponent_points_cache: dict[int, float] = {}
    host_team_ids: dict[int, str] = {}  # team_id -> override key (team name)
    roster_dirty = False

    total = len(roster)
    for idx, entry in enumerate(roster, start=1):
        search_name = entry.get("search_name", "")
        try:
            team_obj, resolved_id = _resolve_sportmonks_team(client, entry)
            if resolved_id is None:
                raise _BootstrapSkip(f"could not resolve a SportMonks id for {search_name!r}")
            if entry.get("sportmonks_team_id") != resolved_id:
                entry["sportmonks_team_id"] = resolved_id
                roster_dirty = True

            mapping = mapper.resolve(team_obj)
            if not isinstance(mapping, TeamFifaMapping):
                raise _BootstrapSkip(
                    f"no FIFA match for {search_name!r} "
                    f"(attempted {mapping.attempted_keys})"
                )

            team_name = team_obj.get("name") or search_name
            override_key = _host_override_key(
                team_name, search_name, mapping.fifa_country_code, host_overrides
            )
            is_host = override_key is not None or mapping.fifa_country_code in _HOST_CODES
            team = Team(
                team_id=resolved_id,
                name=team_name,
                confederation=entry.get("confederation", "UNKNOWN"),
                fifa_points=mapping.fifa_points,
                fifa_rank=mapping.fifa_rank,
                is_host=is_host,
            )
            teams[resolved_id] = team

            logger.info("Bootstrapping %d/%d: %s (%s)...",
                        idx, total, team.name, mapping.fifa_country_code)

            # Hosts play no WC qualifiers: skip the fixture fetch and flag them
            # for a synthetic rating once the baseline is known.
            if override_key is not None:
                matches_by_team[resolved_id] = []
                host_team_ids[resolved_id] = override_key
                logger.info("  -> host nation; using synthetic override (no qualifiers).")
                continue

            fixtures = client.get_fixtures_for_team(
                resolved_id, limit=config.MATCH_WINDOW_SIZE
            )
            matches: list[MatchStats] = []
            for fixture in fixtures:
                parsed = _parse_fixture(
                    fixture, resolved_id, client, mapper, teams,
                    opponent_points_cache,
                )
                if parsed is not None:
                    matches.append(parsed)
            matches_by_team[resolved_id] = matches
            logger.info("  -> %d match(es) parsed for %s.", len(matches), team.name)

        except Exception as exc:  # noqa: BLE001 - one bad team must not abort the rest
            logger.error("FAILED to bootstrap %s: %s", search_name, exc)
            failed_teams.append(search_name)
            continue

    if roster_dirty:
        _save_roster(roster)

    all_matches = [m for ms in matches_by_team.values() for m in ms]
    baseline = compute_baseline_goals(all_matches, teams)
    logger.info(
        "Baseline: %.3f goals/match (%d filtered matches), %.3f/team.",
        baseline.baseline_goals_per_match, baseline.filtered_match_count,
        baseline.baseline_goals_per_team,
    )

    # Hosts have no matches, so their ratings are synthesized from the override
    # file once the baseline (which the formula references) is known.
    host_ratings: dict[int, TeamRating] = {}
    for team_id, override_key in host_team_ids.items():
        host_ratings[team_id] = _build_host_rating(
            teams[team_id], host_overrides[override_key], baseline
        )
        logger.info("Synthesized host rating for %s (attack_final=%.3f, "
                    "defense_final=%.3f).", teams[team_id].name,
                    host_ratings[team_id].attack_final,
                    host_ratings[team_id].defense_final)

    pool = TournamentPool(
        teams=teams,
        matches_by_team=matches_by_team,
        all_matches=all_matches,
        baseline=baseline,
        fifa_release=fifa_release,
        bootstrapped_at=datetime.now(),
        failed_teams=tuple(failed_teams),
        host_ratings=host_ratings,
    )

    _save_pool(pool, cache_path)
    logger.info(
        "Bootstrap complete: %d/%d teams, %d failed%s.",
        len(matches_by_team), total, len(failed_teams),
        f" ({', '.join(failed_teams)})" if failed_teams else "",
    )
    return pool


class _BootstrapSkip(RuntimeError):
    """Internal: a recoverable per-team bootstrap failure."""


# ---------------------------------------------------------------------------
# Host overrides
# ---------------------------------------------------------------------------
def load_host_overrides(
    path: Path = _DEFAULT_HOST_OVERRIDES_PATH,
) -> dict[str, dict[str, Any]]:
    """Load and validate the host overrides file.

    Returns a dict keyed by host team name (e.g. ``"United States"``). Each
    value must carry numeric ``attack_base`` and ``defense_base`` fields.
    """
    payload = json.loads(_absolute(path).read_text(encoding="utf-8"))
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        raise ValueError(f"{path} has no 'hosts' object.")
    for name, info in hosts.items():
        if not isinstance(info, dict):
            raise ValueError(f"Host override {name!r} is not an object.")
        for key in ("attack_base", "defense_base"):
            if not isinstance(info.get(key), (int, float)):
                raise ValueError(
                    f"Host override {name!r} is missing a numeric {key!r}."
                )
    return hosts


def _host_override_key(
    team_name: str,
    search_name: str,
    fifa_code: str,
    host_overrides: dict[str, dict[str, Any]],
) -> str | None:
    """Return the host-overrides key matching this team, or None."""
    if team_name in host_overrides:
        return team_name
    for key, info in host_overrides.items():
        if info.get("search_name") == search_name:
            return key
    return None


def _build_host_rating(
    team: Team, override: dict[str, Any], baseline: TournamentBaseline
) -> TeamRating:
    """Synthesize a TeamRating from a host override.

    The final values fold in the confederation multiplier and HOST_BONUS the
    same way the aggregation pipeline would, so ``predict_match`` needs no
    host-aware logic. ``matches_used=0`` flags the rating as synthetic (and
    triggers the documented low-confidence UserWarning).
    """
    conf_mult = config.CONFEDERATION_MULTIPLIERS.get(team.confederation, 1.0)
    bonus = config.HOST_BONUS
    attack_base = float(override["attack_base"])
    defense_base = float(override["defense_base"])
    per_team = baseline.baseline_goals_per_team

    return TeamRating(
        team_id=team.team_id,
        attack_raw=attack_base * per_team,
        defense_raw=defense_base * per_team,
        attack_normalized=attack_base,
        defense_normalized=defense_base,
        attack_final=attack_base * conf_mult * bonus,
        defense_final=defense_base / (conf_mult * bonus),
        matches_used=0,
    )


def _resolve_sportmonks_team(
    client: SportmonksClient, entry: dict[str, Any]
) -> tuple[dict[str, Any], int | None]:
    """Resolve a roster entry to (full_team_object, sportmonks_team_id)."""
    team_id = entry.get("sportmonks_team_id")
    if team_id is None:
        hits = client.get(f"teams/search/{entry.get('search_name', '')}").get("data")
        if not isinstance(hits, list) or not hits:
            return {}, None
        nationals = [h for h in hits
                     if h.get("type") == "national" and h.get("gender") == "male"]
        team_id = (nationals[0] if nationals else hits[0]).get("id")
        if team_id is None:
            return {}, None

    full = client.get(f"teams/{team_id}", params={"include": "country"}).get("data")
    if not isinstance(full, dict):
        return {}, None
    return full, int(team_id)


def _parse_fixture(
    fixture: dict[str, Any],
    team_id: int,
    client: SportmonksClient,
    mapper: TeamFifaMapper,
    teams: dict[int, Team],
    opponent_points_cache: dict[int, float],
) -> MatchStats | None:
    """Resolve the opponent's FIFA points and parse one fixture to MatchStats."""
    opponent_id, opponent_name = _identify_opponent(fixture, team_id)
    if opponent_id == 0:
        logger.warning("Fixture %s has no opponent for team %s; skipping.",
                       fixture.get("id"), team_id)
        return None

    opponent_points = _resolve_opponent_points(
        opponent_id, opponent_name, client, mapper, teams, opponent_points_cache
    )
    try:
        return parse_fixture_to_match_stats(fixture, team_id, opponent_points)
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Could not parse fixture %s for team %s: %s",
                       fixture.get("id"), team_id, exc)
        return None


def _resolve_opponent_points(
    opponent_id: int,
    opponent_name: str,
    client: SportmonksClient,
    mapper: TeamFifaMapper,
    teams: dict[int, Team],
    cache: dict[int, float],
) -> float:
    """FIFA points for an opponent. Uses current rankings (historical = v2).

    Falls back to ``REFERENCE_FIFA_POINTS`` when the opponent cannot be mapped,
    so one unmappable opponent never sinks an otherwise-valid match.
    """
    if opponent_id in teams:
        return teams[opponent_id].fifa_points
    if opponent_id in cache:
        return cache[opponent_id]

    points = float(config.REFERENCE_FIFA_POINTS)
    try:
        obj = client.get(f"teams/{opponent_id}", params={"include": "country"}).get("data")
        if isinstance(obj, dict):
            mapping = mapper.resolve(obj)
            if isinstance(mapping, TeamFifaMapping):
                points = mapping.fifa_points
                # Record the opponent so the baseline can evaluate its gap.
                teams.setdefault(opponent_id, Team(
                    team_id=opponent_id,
                    name=obj.get("name") or opponent_name,
                    confederation="UNKNOWN",
                    fifa_points=mapping.fifa_points,
                    fifa_rank=mapping.fifa_rank,
                    is_host=False,
                ))
            else:
                logger.warning("Opponent %s (%s) unmapped; using reference points.",
                               opponent_id, opponent_name)
    except SportmonksError as exc:
        logger.warning("Could not fetch opponent %s (%s): %s; using reference points.",
                       opponent_id, opponent_name, exc)

    cache[opponent_id] = points
    return points


def _identify_opponent(fixture: dict[str, Any], team_id: int) -> tuple[int, str]:
    """Return (opponent_id, opponent_name) for the non-``team_id`` participant."""
    for participant in fixture.get("participants", []):
        if participant.get("id") != team_id:
            return int(participant.get("id", 0)), str(participant.get("name", ""))
    return 0, ""


# ===========================================================================
# Prediction
# ===========================================================================
def predict_matchup(
    team_a_name: str,
    team_b_name: str,
    pool: TournamentPool | None = None,
) -> str:
    """Predict a single matchup by team name; return the JSON report string."""
    if pool is None:
        pool = bootstrap_tournament_pool()
    team_a = _resolve_team_by_name(team_a_name, pool)
    team_b = _resolve_team_by_name(team_b_name, pool)
    return _predict_for_teams(team_a, team_b, pool)


def predict_matchup_by_id(
    team_a_id: int,
    team_b_id: int,
    pool: TournamentPool | None = None,
    *,
    player_display_cache: dict[str, Any] | None = None,
) -> str:
    """Predict a single matchup by SportMonks team id; return the JSON report.

    Resolving by id sidesteps the name-format mismatches between the fixtures
    table (e.g. "Czech Republic", "Cape Verde Islands") and the pool's
    SportMonks team names, which silently skipped fixtures under name lookup.
    """
    if pool is None:
        pool = bootstrap_tournament_pool()
    team_a = _resolve_team_by_id(team_a_id, pool)
    team_b = _resolve_team_by_id(team_b_id, pool)
    return _predict_for_teams(
        team_a, team_b, pool, player_display_cache=player_display_cache
    )


def _predict_for_teams(
    team_a: Team,
    team_b: Team,
    pool: TournamentPool,
    *,
    player_display_cache: dict[str, Any] | None = None,
) -> str:
    """Shared prediction core: two resolved teams -> JSON matchup report."""
    host_overrides = load_host_overrides()
    rating_a, matches_a, source_a, reason_a = _rating_for(team_a, pool, host_overrides)
    rating_b, matches_b, source_b, reason_b = _rating_for(team_b, pool, host_overrides)

    for team, rating in ((team_a, rating_a), (team_b, rating_b)):
        if rating.attack_final == 0.0 and rating.defense_final == 0.0:
            logger.warning(
                "%s has an all-zero rating: its source fixtures carry no "
                "statistics (a SportMonks data-coverage gap for this "
                "confederation's qualifiers). The prediction will be degenerate.",
                team.name,
            )

    prediction = predict_match(rating_a, rating_b, pool.baseline)
    report = build_matchup_report(
        team_a, team_b, matches_a, matches_b, rating_a, rating_b, prediction,
        team_a_data_source=source_a, team_b_data_source=source_b,
        team_a_host_reasoning=reason_a, team_b_host_reasoning=reason_b,
    )
    player_ratings = _build_display_player_ratings(
        team_a, team_b, cache=player_display_cache
    )
    return matchup_report_to_json(replace(report, player_ratings=player_ratings))


def _build_display_player_ratings(
    team_a: Team,
    team_b: Team,
    *,
    cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build projected XI + squad display blocks; never raises.

    Reads squad and id_map from Neon; ratings from SportMonks or manual fallback.
    Reuses per-team blocks from ``cache`` (keyed by team_code) within a run.
    """
    try:
        client = SportmonksClient()
        return build_matchup_player_display(
            client,
            team_a.team_id,
            team_b.team_id,
            cache=cache,
        )
    except Exception as exc:  # noqa: BLE001 - display-only; must not block prediction
        logger.warning(
            "Could not build player_ratings for %s vs %s: %s",
            team_a.name,
            team_b.name,
            exc,
        )
        empty = empty_team_player_display()
        return {"team_a": dict(empty), "team_b": dict(empty)}


def _rating_for(
    team: Team,
    pool: TournamentPool,
    host_overrides: dict[str, dict[str, Any]],
) -> tuple[TeamRating, list[MatchStats], str, str | None]:
    """Resolve a team's rating, matches, data source and host reasoning.

    Hosts use the synthetic override rating (no qualifier matches); everyone
    else derives a rating from their recent qualifier matches.
    """
    if team.team_id in pool.host_ratings:
        reasoning = host_overrides.get(team.name, {}).get("reasoning")
        return pool.host_ratings[team.team_id], [], _DATA_SOURCE_HOST, reasoning

    matches = pool.matches_by_team.get(team.team_id, [])
    if not matches:
        raise ValueError(f"No recent matches available for {team.name!r}.")
    rating = compute_team_rating(team, matches, pool.baseline)
    return rating, matches, _DATA_SOURCE_QUALIFIER, None


def _resolve_team_by_id(team_id: int, pool: TournamentPool) -> Team:
    """Look up a team by its SportMonks id among teams that have a rating path.

    Only the rostered tournament teams (those with a match window or a synthetic
    host rating) are predictable; opponent-only ids registered for the baseline
    are rejected so they fail loudly rather than producing an empty rating.
    """
    if team_id in pool.host_ratings:
        return pool.teams[team_id]
    if team_id in pool.matches_by_team and team_id in pool.teams:
        return pool.teams[team_id]
    raise ValueError(f"No rostered team with id {team_id} in the pool.")


def _resolve_team_by_name(name: str, pool: TournamentPool) -> Team:
    """Case-insensitive name lookup among teams that have match data."""
    candidates = {
        pool.teams[tid].name: pool.teams[tid]
        for tid in pool.matches_by_team
        if tid in pool.teams
    }
    target = name.strip().casefold()
    for team_name, team in candidates.items():
        if team_name.casefold() == target:
            return team

    suggestions = difflib.get_close_matches(name, list(candidates), n=3, cutoff=0.5)
    hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
    raise ValueError(f'No team named "{name}".{hint}')


# ===========================================================================
# Serialization
# ===========================================================================
def _load_roster() -> list[dict[str, Any]]:
    payload = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    teams = payload.get("teams")
    if not isinstance(teams, list):
        raise ValueError(f"{_ROSTER_PATH} has no 'teams' list.")
    return teams


def _save_roster(teams: list[dict[str, Any]]) -> None:
    payload = json.loads(_ROSTER_PATH.read_text(encoding="utf-8"))
    payload["teams"] = teams
    _ROSTER_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info("Updated %s with resolved SportMonks ids.", _ROSTER_PATH.name)


def _load_pool_if_fresh(cache_path: Path, ttl_hours: int) -> TournamentPool | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None
    if payload.get("schema_version") != _POOL_SCHEMA_VERSION:
        return None
    try:
        bootstrapped_at = datetime.fromisoformat(payload["bootstrapped_at"])
    except (KeyError, ValueError):
        return None
    if datetime.now() - bootstrapped_at > timedelta(hours=ttl_hours):
        logger.info("Tournament pool cache is stale (> %dh); rebuilding.", ttl_hours)
        return None
    try:
        return _pool_from_dict(payload, bootstrapped_at)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not deserialize cached pool: %s; rebuilding.", exc)
        return None


def _save_pool(pool: TournamentPool, cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": _POOL_SCHEMA_VERSION,
        "bootstrapped_at": pool.bootstrapped_at.isoformat(),
        "fifa_date_id": pool.fifa_release.date_id,
        "failed_teams": list(pool.failed_teams),
        "teams": {str(tid): _team_to_dict(t) for tid, t in pool.teams.items()},
        "matches_by_team": {
            str(tid): [_match_to_dict(m) for m in matches]
            for tid, matches in pool.matches_by_team.items()
        },
        "host_ratings": {
            str(tid): _teamrating_to_dict(r) for tid, r in pool.host_ratings.items()
        },
    }
    cache_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info("Saved tournament pool to %s.", cache_path)


def _pool_from_dict(payload: dict[str, Any], bootstrapped_at: datetime) -> TournamentPool:
    teams = {
        int(tid): _team_from_dict(t)
        for tid, t in payload.get("teams", {}).items()
    }
    matches_by_team = {
        int(tid): [_match_from_dict(m) for m in matches]
        for tid, matches in payload.get("matches_by_team", {}).items()
    }
    all_matches = [m for ms in matches_by_team.values() for m in ms]

    fifa_release = FifaRankingsClient().fetch_ranking(payload["fifa_date_id"])
    baseline = compute_baseline_goals(all_matches, teams)

    # matches_used=0 ratings would re-trigger the synthetic UserWarning on every
    # cache load; suppress it here since the warning already fired at bootstrap.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        host_ratings = {
            int(tid): _teamrating_from_dict(r)
            for tid, r in payload.get("host_ratings", {}).items()
        }

    return TournamentPool(
        teams=teams,
        matches_by_team=matches_by_team,
        all_matches=all_matches,
        baseline=baseline,
        fifa_release=fifa_release,
        bootstrapped_at=bootstrapped_at,
        failed_teams=tuple(payload.get("failed_teams", [])),
        host_ratings=host_ratings,
    )


def _team_to_dict(team: Team) -> dict[str, Any]:
    return {
        "team_id": team.team_id, "name": team.name,
        "confederation": team.confederation, "fifa_points": team.fifa_points,
        "fifa_rank": team.fifa_rank, "is_host": team.is_host,
    }


def _team_from_dict(d: dict[str, Any]) -> Team:
    return Team(
        team_id=int(d["team_id"]), name=d["name"],
        confederation=d["confederation"], fifa_points=float(d["fifa_points"]),
        fifa_rank=int(d["fifa_rank"]), is_host=bool(d["is_host"]),
    )


def _teamrating_to_dict(r: TeamRating) -> dict[str, Any]:
    return {
        "team_id": r.team_id, "attack_raw": r.attack_raw,
        "defense_raw": r.defense_raw, "attack_normalized": r.attack_normalized,
        "defense_normalized": r.defense_normalized, "attack_final": r.attack_final,
        "defense_final": r.defense_final, "matches_used": r.matches_used,
    }


def _teamrating_from_dict(d: dict[str, Any]) -> TeamRating:
    return TeamRating(
        team_id=int(d["team_id"]), attack_raw=float(d["attack_raw"]),
        defense_raw=float(d["defense_raw"]),
        attack_normalized=float(d["attack_normalized"]),
        defense_normalized=float(d["defense_normalized"]),
        attack_final=float(d["attack_final"]),
        defense_final=float(d["defense_final"]),
        matches_used=int(d["matches_used"]),
    )


def _match_to_dict(m: MatchStats) -> dict[str, Any]:
    return {
        "match_id": m.match_id, "date": m.date.isoformat(),
        "team_id": m.team_id, "opponent_id": m.opponent_id,
        "opponent_fifa_points": m.opponent_fifa_points,
        "competition_type": m.competition_type, "venue": m.venue,
        "goals_scored": m.goals_scored, "xg_created": m.xg_created,
        "big_chances_created": m.big_chances_created,
        "shots_on_target": m.shots_on_target, "xgot_created": m.xgot_created,
        "goals_conceded": m.goals_conceded, "xg_conceded": m.xg_conceded,
        "big_chances_conceded": m.big_chances_conceded,
        "shots_on_target_conceded": m.shots_on_target_conceded,
        "xgot_conceded": m.xgot_conceded,
        "possession_pct": m.possession_pct, "opponent_name": m.opponent_name,
    }


def _match_from_dict(d: dict[str, Any]) -> MatchStats:
    return MatchStats(
        match_id=int(d["match_id"]), date=datetime.fromisoformat(d["date"]),
        team_id=int(d["team_id"]), opponent_id=int(d["opponent_id"]),
        opponent_fifa_points=float(d["opponent_fifa_points"]),
        competition_type=d["competition_type"], venue=d["venue"],
        goals_scored=int(d["goals_scored"]), xg_created=float(d["xg_created"]),
        big_chances_created=int(d["big_chances_created"]),
        shots_on_target=int(d["shots_on_target"]),
        xgot_created=float(d["xgot_created"]),
        goals_conceded=int(d["goals_conceded"]),
        xg_conceded=float(d["xg_conceded"]),
        big_chances_conceded=int(d["big_chances_conceded"]),
        shots_on_target_conceded=int(d["shots_on_target_conceded"]),
        xgot_conceded=float(d["xgot_conceded"]),
        possession_pct=float(d.get("possession_pct", 0.0)),
        opponent_name=d.get("opponent_name", ""),
    )


def _absolute(path: Path) -> Path:
    return path if path.is_absolute() else _PROJECT_ROOT / path


# ===========================================================================
# CLI
# ===========================================================================
if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Predict a World Cup 2026 matchup."
    )
    parser.add_argument("team_a", help="First team name (e.g., 'Brazil')")
    parser.add_argument("team_b", help="Second team name (e.g., 'France')")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Bypass the tournament pool cache")
    args = parser.parse_args()

    pool = bootstrap_tournament_pool(force_refresh=args.force_refresh)
    print(predict_matchup(args.team_a, args.team_b, pool))
