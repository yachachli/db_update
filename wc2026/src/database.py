"""Neon Postgres read/write layer for the World Cup 2026 predictive model.

All database operations go through this module. Callers pass domain objects
(:class:`~src.models.Team`, :class:`~src.models.TeamRating`, etc.) and receive
plain dicts on read paths.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any, Generator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.config import NEON_DATABASE_URL
from src.fifa_rankings import FifaRankingEntry
from src.models import Team, TeamRating
from src.reporting import MatchupReport, matchup_report_to_dict

__all__ = [
    "DatabaseError",
    "get_connection",
    "upsert_team",
    "upsert_team_rating",
    "upsert_fifa_ranking",
    "upsert_fixture",
    "upsert_prediction",
    "get_upcoming_fixtures",
    "get_fixtures_needing_prediction",
    "get_prediction",
    "get_all_teams",
    "get_team_rating",
]

logger = logging.getLogger(__name__)


class DatabaseError(RuntimeError):
    """Raised when the database is unavailable or a query fails."""


def _normalize_db_url(url: str) -> str:
    """Strip query params that break psycopg on GitHub Actions (e.g. channel_binding)."""
    parsed = urlparse(url)
    if not parsed.query:
        return url
    filtered = [(k, v) for k, v in parse_qsl(parsed.query) if k != "channel_binding"]
    return urlunparse(parsed._replace(query=urlencode(filtered)))


def _require_url() -> str:
    if not NEON_DATABASE_URL:
        raise DatabaseError(
            "NEON_DATABASE_URL is not set. Add it to your .env file "
            "(see .env.example)."
        )
    return _normalize_db_url(NEON_DATABASE_URL)


@contextmanager
def get_connection() -> Generator[psycopg.Connection[Any], None, None]:
    """Yield a psycopg connection from ``NEON_DATABASE_URL``. Auto-closes."""
    conn = psycopg.connect(_require_url(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def upsert_team(team: Team, *, fifa_code: str | None = None) -> None:
    """Insert or update a team row keyed by ``team_id``."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO teams (team_id, name, confederation, fifa_code, is_host)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (team_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    confederation = EXCLUDED.confederation,
                    fifa_code = EXCLUDED.fifa_code,
                    is_host = EXCLUDED.is_host
                """,
                (team.team_id, team.name, team.confederation, fifa_code, team.is_host),
            )
    logger.debug("Upserted team %s (id=%d)", team.name, team.team_id)


def upsert_team_rating(rating: TeamRating, data_source: str) -> None:
    """Insert or replace the current rating for a team (one row per team)."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO team_ratings (
                    team_id, attack_final, defense_final, matches_used,
                    data_source, computed_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (team_id) DO UPDATE SET
                    attack_final = EXCLUDED.attack_final,
                    defense_final = EXCLUDED.defense_final,
                    matches_used = EXCLUDED.matches_used,
                    data_source = EXCLUDED.data_source,
                    computed_at = NOW()
                """,
                (
                    rating.team_id,
                    rating.attack_final,
                    rating.defense_final,
                    rating.matches_used,
                    data_source,
                ),
            )
    logger.debug(
        "Upserted rating for team_id=%d (attack=%.3f, defense=%.3f, source=%s)",
        rating.team_id, rating.attack_final, rating.defense_final, data_source,
    )


def upsert_fifa_ranking(entry: FifaRankingEntry, release_date: date) -> None:
    """Insert or update a FIFA ranking row (composite key on code + date)."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO fifa_rankings (fifa_code, rank, points, release_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (fifa_code, release_date) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    points = EXCLUDED.points
                """,
                (entry.country_code, entry.rank, entry.points, release_date),
            )
    logger.debug(
        "Upserted FIFA ranking %s rank=%d points=%.2f (%s)",
        entry.country_code, entry.rank, entry.points, release_date,
    )


def upsert_fixture(fixture_data: dict[str, Any]) -> int:
    """Insert or update a WC 2026 fixture. Returns the internal ``fixture_id``.

    ``fixture_data`` must include: ``sportmonks_fixture_id``, ``team_a_id``,
    ``team_b_id``, ``scheduled_at``, ``venue_city``, ``round``, ``status``.
    Optional: ``team_a_name``, ``team_b_name``, ``actual_home_goals``,
    ``actual_away_goals``, ``actual_outcome``.
    """
    required = (
        "sportmonks_fixture_id", "team_a_id", "team_b_id",
        "scheduled_at", "venue_city", "round", "status",
    )
    missing = [k for k in required if k not in fixture_data]
    if missing:
        raise DatabaseError(f"upsert_fixture missing required keys: {missing}")

    with get_connection() as conn:
        with conn.transaction():
            row = conn.execute(
                """
                INSERT INTO wc2026_fixtures (
                    sportmonks_fixture_id, team_a_id, team_b_id,
                    team_a_name, team_b_name, scheduled_at, venue_city, round,
                    actual_home_goals, actual_away_goals, actual_outcome, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sportmonks_fixture_id) DO UPDATE SET
                    team_a_id = EXCLUDED.team_a_id,
                    team_b_id = EXCLUDED.team_b_id,
                    team_a_name = EXCLUDED.team_a_name,
                    team_b_name = EXCLUDED.team_b_name,
                    scheduled_at = EXCLUDED.scheduled_at,
                    venue_city = EXCLUDED.venue_city,
                    round = EXCLUDED.round,
                    actual_home_goals = EXCLUDED.actual_home_goals,
                    actual_away_goals = EXCLUDED.actual_away_goals,
                    actual_outcome = EXCLUDED.actual_outcome,
                    status = EXCLUDED.status
                RETURNING fixture_id
                """,
                (
                    fixture_data["sportmonks_fixture_id"],
                    fixture_data["team_a_id"],
                    fixture_data["team_b_id"],
                    fixture_data.get("team_a_name"),
                    fixture_data.get("team_b_name"),
                    fixture_data["scheduled_at"],
                    fixture_data["venue_city"],
                    fixture_data["round"],
                    fixture_data.get("actual_home_goals"),
                    fixture_data.get("actual_away_goals"),
                    fixture_data.get("actual_outcome"),
                    fixture_data["status"],
                ),
            ).fetchone()

    if row is None:
        raise DatabaseError("upsert_fixture did not return fixture_id")
    fixture_id = int(row["fixture_id"])
    logger.debug(
        "Upserted fixture id=%d (sportmonks=%s)",
        fixture_id, fixture_data["sportmonks_fixture_id"],
    )
    return fixture_id


def upsert_prediction(
    fixture_id: int,
    report: MatchupReport | dict[str, Any],
    *,
    team_a_id: int | None = None,
    team_b_id: int | None = None,
) -> None:
    """Insert or replace the prediction for a fixture.

    Accepts either a :class:`MatchupReport` or the JSON dict returned by
    ``predict_matchup()`` (requires ``team_a_id`` / ``team_b_id`` when passing
    a dict, since those IDs live on the fixture row, not in the report JSON).
    """
    if isinstance(report, dict):
        if team_a_id is None or team_b_id is None:
            raise DatabaseError(
                "team_a_id and team_b_id are required when upserting from a dict."
            )
        pred = report["prediction"]
        probs = pred["win_probabilities"]
        xg = pred["expected_goals"]
        scoreline = pred["most_likely_scoreline"]
        most_likely = scoreline.get("as_string") or (
            f"{scoreline['team_a_goals']}-{scoreline['team_b_goals']}"
        )
        params = (
            fixture_id,
            team_a_id,
            team_b_id,
            probs["team_a_win"],
            probs["draw"],
            probs["team_b_win"],
            xg["team_a"],
            xg["team_b"],
            most_likely,
            Jsonb(report),
        )
    else:
        pred = report.prediction
        score_a, score_b = pred.most_likely_scoreline
        most_likely = f"{score_a}-{score_b}"
        full_report = matchup_report_to_dict(report)
        params = (
            fixture_id,
            pred.team_a_id,
            pred.team_b_id,
            pred.prob_a_win,
            pred.prob_draw,
            pred.prob_b_win,
            pred.xg_a,
            pred.xg_b,
            most_likely,
            Jsonb(full_report),
        )

    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO predictions (
                    fixture_id, team_a_id, team_b_id,
                    prob_a_win, prob_draw, prob_b_win,
                    xg_a, xg_b, most_likely_score, full_report, predicted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (fixture_id) DO UPDATE SET
                    team_a_id = EXCLUDED.team_a_id,
                    team_b_id = EXCLUDED.team_b_id,
                    prob_a_win = EXCLUDED.prob_a_win,
                    prob_draw = EXCLUDED.prob_draw,
                    prob_b_win = EXCLUDED.prob_b_win,
                    xg_a = EXCLUDED.xg_a,
                    xg_b = EXCLUDED.xg_b,
                    most_likely_score = EXCLUDED.most_likely_score,
                    full_report = EXCLUDED.full_report,
                    predicted_at = NOW()
                """,
                params,
            )
    logger.debug("Upserted prediction for fixture_id=%d", fixture_id)


def get_upcoming_fixtures(within_days: int = 3) -> list[dict[str, Any]]:
    """Fixtures scheduled within the next ``within_days``, status ``scheduled``."""
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=within_days)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM wc2026_fixtures
            WHERE status = 'scheduled'
              AND scheduled_at >= %s
              AND scheduled_at <= %s
            ORDER BY scheduled_at ASC
            """,
            (now, cutoff),
        ).fetchall()
    return list(rows)


def get_fixtures_needing_prediction(
    within_days: int = 3,
    prediction_max_age_hours: int = 24,
) -> list[dict[str, Any]]:
    """Fixtures due for a fresh prediction (scheduled, upcoming, stale/missing)."""
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=within_days)
    max_age = timedelta(hours=prediction_max_age_hours)
    stale_before = now - max_age

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT f.*
            FROM wc2026_fixtures f
            LEFT JOIN predictions p ON p.fixture_id = f.fixture_id
            WHERE f.status = 'scheduled'
              AND f.scheduled_at >= %s
              AND f.scheduled_at <= %s
              AND (
                  p.fixture_id IS NULL
                  OR p.predicted_at < %s
              )
            ORDER BY f.scheduled_at ASC
            """,
            (now, cutoff, stale_before),
        ).fetchall()
    return list(rows)


def get_prediction(fixture_id: int) -> dict[str, Any] | None:
    """Return the prediction row for ``fixture_id``, or ``None``."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM predictions WHERE fixture_id = %s",
            (fixture_id,),
        ).fetchone()
    return dict(row) if row else None


def get_all_teams() -> list[dict[str, Any]]:
    """Return all teams ordered by name."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM teams ORDER BY name ASC"
        ).fetchall()
    return list(rows)


def get_team_rating(team_id: int) -> dict[str, Any] | None:
    """Return the current rating row for ``team_id``, or ``None``."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM team_ratings WHERE team_id = %s",
            (team_id,),
        ).fetchone()
    return dict(row) if row else None
