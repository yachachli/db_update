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
    "upsert_wc2026_squad_row",
    "upsert_wc2026_squad_rows",
    "get_wc2026_squad_team_codes",
    "get_wc2026_squad_for_team",
    "upsert_player_id_map_row",
    "replace_player_match_review_for_team",
    "upsert_team_player_ratings",
    "get_team_player_ratings_for_display",
    "get_fifa_code_for_team_id",
    "upsert_player_ratings_history_rows",
    "get_player_ratings_snapshot_summary",
    "get_team_id_for_fifa_code",
    "get_player_id_map_for_team",
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


_SQUAD_UPSERT_SQL = """
    INSERT INTO wc2026_squads (
        team_code, team_name, squad_no, position, player_name,
        first_names, last_names, name_on_shirt, dob, club,
        club_country, height_cm
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_code, squad_no) DO UPDATE SET
        team_name = EXCLUDED.team_name,
        position = EXCLUDED.position,
        player_name = EXCLUDED.player_name,
        first_names = EXCLUDED.first_names,
        last_names = EXCLUDED.last_names,
        name_on_shirt = EXCLUDED.name_on_shirt,
        dob = EXCLUDED.dob,
        club = EXCLUDED.club,
        club_country = EXCLUDED.club_country,
        height_cm = EXCLUDED.height_cm
"""


def _squad_row_params(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["team_code"],
        row["team_name"],
        int(row["squad_no"]),
        row["position"],
        row["player_name"],
        row.get("first_names") or None,
        row.get("last_names") or None,
        row.get("name_on_shirt") or None,
        row.get("dob") or None,
        row.get("club") or None,
        row.get("club_country") or None,
        int(row["height_cm"]) if row.get("height_cm") else None,
    )


def upsert_wc2026_squad_row(row: dict[str, Any]) -> None:
    """Insert or update one FIFA squad row (keyed by team_code + squad_no)."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(_SQUAD_UPSERT_SQL, _squad_row_params(row))


def upsert_wc2026_squad_rows(rows: list[dict[str, Any]]) -> None:
    """Batch UPSERT squad rows in a single transaction."""
    with get_connection() as conn:
        with conn.transaction():
            for row in rows:
                conn.execute(_SQUAD_UPSERT_SQL, _squad_row_params(row))


def get_wc2026_squad_team_codes() -> list[str]:
    """Return distinct team_code values from wc2026_squads, sorted."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT team_code
            FROM wc2026_squads
            ORDER BY team_code ASC
            """
        ).fetchall()
    return [str(row["team_code"]) for row in rows]


def get_wc2026_squad_for_team(team_code: str) -> list[dict[str, Any]]:
    """Return all squad rows for one team_code, ordered by squad_no."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM wc2026_squads
            WHERE team_code = %s
            ORDER BY squad_no ASC
            """,
            (team_code,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_player_id_map_for_team(team_code: str) -> list[dict[str, Any]]:
    """Return player_id_map rows for one team_code."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT sportmonks_player_id, team_code, squad_no,
                   match_method, match_confidence
            FROM player_id_map
            WHERE team_code = %s
            ORDER BY squad_no ASC
            """,
            (team_code,),
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_player_id_map_row(
    *,
    sportmonks_player_id: int,
    team_code: str,
    squad_no: int,
    match_method: str,
    match_confidence: float,
) -> None:
    """Insert or update a SportMonks player -> FIFA squad mapping."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO player_id_map (
                    sportmonks_player_id, team_code, squad_no,
                    match_method, match_confidence, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (sportmonks_player_id) DO UPDATE SET
                    team_code = EXCLUDED.team_code,
                    squad_no = EXCLUDED.squad_no,
                    match_method = EXCLUDED.match_method,
                    match_confidence = EXCLUDED.match_confidence,
                    updated_at = NOW()
                """,
                (
                    sportmonks_player_id,
                    team_code,
                    squad_no,
                    match_method,
                    match_confidence,
                ),
            )


def replace_player_match_review_for_team(
    team_code: str,
    rows: list[dict[str, Any]],
) -> None:
    """Replace audit rows for one team in player_match_review."""
    with get_connection() as conn:
        with conn.transaction():
            conn.execute(
                "DELETE FROM player_match_review WHERE team_code = %s",
                (team_code,),
            )
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO player_match_review (
                        sportmonks_player_id, team_code, sm_name, sm_dob,
                        reason, detail, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (sportmonks_player_id, team_code) DO UPDATE SET
                        sm_name = EXCLUDED.sm_name,
                        sm_dob = EXCLUDED.sm_dob,
                        reason = EXCLUDED.reason,
                        detail = EXCLUDED.detail,
                        updated_at = NOW()
                    """,
                    (
                        int(row["sportmonks_player_id"]),
                        team_code,
                        row["sm_name"],
                        row.get("sm_dob"),
                        row["reason"],
                        row.get("detail"),
                    ),
                )


def get_team_id_for_fifa_code(fifa_code: str) -> int | None:
    """Return SportMonks team_id for a FIFA team_code, if stored in teams."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT team_id FROM teams WHERE fifa_code = %s",
            (fifa_code,),
        ).fetchone()
    if not row:
        return None
    return int(row["team_id"])


def get_fifa_code_for_team_id(team_id: int) -> str | None:
    """Return FIFA team_code for a SportMonks team_id, if stored in teams."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT fifa_code FROM teams WHERE team_id = %s",
            (team_id,),
        ).fetchone()
    if not row or not row.get("fifa_code"):
        return None
    return str(row["fifa_code"])


def upsert_team_player_ratings(rows: list[dict[str, Any]]) -> None:
    """Batch UPSERT manual or fallback per-player rating averages."""
    if not rows:
        return
    with get_connection() as conn:
        with conn.transaction():
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO team_player_ratings (
                        team_code, squad_no, player_name, avg_rating,
                        matches_counted, source, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (team_code, squad_no) DO UPDATE SET
                        player_name = EXCLUDED.player_name,
                        avg_rating = EXCLUDED.avg_rating,
                        matches_counted = EXCLUDED.matches_counted,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                    """,
                    (
                        row["team_code"],
                        int(row["squad_no"]),
                        row["player_name"],
                        float(row["avg_rating"]),
                        int(row["matches_counted"]),
                        row["source"],
                    ),
                )


def get_team_player_ratings_for_display(
    team_code: str,
    *,
    min_appearances: int = 1,
) -> dict[str, Any] | None:
    """Return player_ratings report shape from persisted team_player_ratings."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT squad_no, player_name, avg_rating, matches_counted
            FROM team_player_ratings
            WHERE team_code = %s
            ORDER BY squad_no ASC
            """,
            (team_code,),
        ).fetchall()
    if not rows:
        return None

    listed: list[dict[str, Any]] = []
    insufficient: list[dict[str, Any]] = []
    for row in rows:
        entry: dict[str, Any] = {
            "player_id": 0,
            "player_name": str(row["player_name"]),
            "avg_rating": round(float(row["avg_rating"]), 2),
            "matches_counted": int(row["matches_counted"]),
            "dob": None,
            "squad_no": int(row["squad_no"]),
            "source": "manual",
            "minutes_share": None,
        }
        if int(row["matches_counted"]) >= min_appearances:
            listed.append(entry)
        else:
            insufficient.append({**entry, "status": "insufficient_data"})

    listed.sort(key=lambda r: r["avg_rating"], reverse=True)
    insufficient.sort(
        key=lambda r: (-r["matches_counted"], r["player_name"].lower()),
    )
    return {
        "source": "manual",
        "listed": listed,
        "insufficient_data": insufficient,
        "window_start_date": None,
        "window_end_date": None,
    }


def upsert_player_ratings_history_rows(rows: list[dict[str, Any]]) -> int:
    """Batch UPSERT daily player rating snapshots. Returns rows written."""
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.transaction():
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO player_ratings_history (
                        entity_key, sportmonks_player_id, team_code,
                        manual_squad_no, player_name, avg_rating, minutes_share,
                        matches_counted, source, window_start_date,
                        window_end_date, snapshot_date, computed_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (entity_key, snapshot_date) DO UPDATE SET
                        sportmonks_player_id = EXCLUDED.sportmonks_player_id,
                        team_code = EXCLUDED.team_code,
                        manual_squad_no = EXCLUDED.manual_squad_no,
                        player_name = EXCLUDED.player_name,
                        avg_rating = EXCLUDED.avg_rating,
                        minutes_share = EXCLUDED.minutes_share,
                        matches_counted = EXCLUDED.matches_counted,
                        source = EXCLUDED.source,
                        window_start_date = EXCLUDED.window_start_date,
                        window_end_date = EXCLUDED.window_end_date,
                        computed_at = NOW()
                    """,
                    (
                        row["entity_key"],
                        row.get("sportmonks_player_id"),
                        row["team_code"],
                        row.get("manual_squad_no"),
                        row["player_name"],
                        float(row["avg_rating"]),
                        row.get("minutes_share"),
                        int(row["matches_counted"]),
                        row["source"],
                        row.get("window_start_date"),
                        row.get("window_end_date"),
                        row["snapshot_date"],
                    ),
                )
    return len(rows)


def get_player_ratings_snapshot_summary(
    snapshot_date: date | None = None,
) -> dict[str, Any]:
    """Return row counts by team and source for one snapshot_date (default today)."""
    snap = snapshot_date or date.today()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT team_code, source, COUNT(*) AS n
            FROM player_ratings_history
            WHERE snapshot_date = %s
            GROUP BY team_code, source
            ORDER BY team_code ASC, source ASC
            """,
            (snap,),
        ).fetchall()
        sample_arg = conn.execute(
            """
            SELECT entity_key, team_code, player_name, avg_rating,
                   minutes_share, matches_counted, source, manual_squad_no
            FROM player_ratings_current
            WHERE team_code = 'ARG'
            ORDER BY avg_rating DESC
            LIMIT 5
            """,
        ).fetchall()
        sample_nzl = conn.execute(
            """
            SELECT entity_key, team_code, player_name, avg_rating,
                   minutes_share, matches_counted, source, manual_squad_no
            FROM player_ratings_current
            WHERE team_code = 'NZL'
            ORDER BY avg_rating DESC
            LIMIT 5
            """,
        ).fetchall()
    return {
        "snapshot_date": snap.isoformat(),
        "by_team_source": [dict(r) for r in rows],
        "sample_arg": [dict(r) for r in sample_arg],
        "sample_nzl": [dict(r) for r in sample_nzl],
    }
