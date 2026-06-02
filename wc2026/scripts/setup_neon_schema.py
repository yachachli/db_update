"""Create Neon Postgres schema (idempotent).

Run from the project root:

    python scripts/setup_neon_schema.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402

from src.config import NEON_DATABASE_URL  # noqa: E402
from src.database import _require_url  # noqa: E402

_TABLES = (
    "teams",
    "team_ratings",
    "fifa_rankings",
    "wc2026_fixtures",
    "predictions",
)

_SCHEMA_STATEMENTS: list[tuple[str, str]] = [
    ("teams", """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            confederation TEXT,
            fifa_code TEXT,
            is_host BOOLEAN DEFAULT FALSE
        )
    """),
    ("team_ratings", """
        CREATE TABLE IF NOT EXISTS team_ratings (
            team_id INTEGER PRIMARY KEY REFERENCES teams(team_id),
            attack_final REAL NOT NULL,
            defense_final REAL NOT NULL,
            matches_used INTEGER NOT NULL,
            data_source TEXT NOT NULL,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """),
    ("fifa_rankings", """
        CREATE TABLE IF NOT EXISTS fifa_rankings (
            fifa_code TEXT NOT NULL,
            rank INTEGER NOT NULL,
            points REAL NOT NULL,
            release_date DATE NOT NULL,
            PRIMARY KEY (fifa_code, release_date)
        )
    """),
    ("wc2026_fixtures", """
        CREATE TABLE IF NOT EXISTS wc2026_fixtures (
            fixture_id SERIAL PRIMARY KEY,
            sportmonks_fixture_id INTEGER UNIQUE NOT NULL,
            team_a_id INTEGER NOT NULL REFERENCES teams(team_id),
            team_b_id INTEGER NOT NULL REFERENCES teams(team_id),
            team_a_name TEXT,
            team_b_name TEXT,
            scheduled_at TIMESTAMPTZ NOT NULL,
            venue_city TEXT,
            round TEXT,
            actual_home_goals INTEGER,
            actual_away_goals INTEGER,
            actual_outcome TEXT,
            status TEXT NOT NULL DEFAULT 'scheduled'
        )
    """),
    ("idx_fixtures_scheduled_at", """
        CREATE INDEX IF NOT EXISTS idx_fixtures_scheduled_at
            ON wc2026_fixtures(scheduled_at)
    """),
    ("idx_fixtures_status", """
        CREATE INDEX IF NOT EXISTS idx_fixtures_status
            ON wc2026_fixtures(status)
    """),
    ("predictions", """
        CREATE TABLE IF NOT EXISTS predictions (
            fixture_id INTEGER PRIMARY KEY REFERENCES wc2026_fixtures(fixture_id),
            team_a_id INTEGER NOT NULL,
            team_b_id INTEGER NOT NULL,
            prob_a_win REAL NOT NULL,
            prob_draw REAL NOT NULL,
            prob_b_win REAL NOT NULL,
            xg_a REAL NOT NULL,
            xg_b REAL NOT NULL,
            most_likely_score TEXT,
            full_report JSONB NOT NULL,
            predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """),
    ("idx_predictions_predicted_at", """
        CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at
            ON predictions(predicted_at DESC)
    """),
]


def _table_exists(conn: psycopg.Connection, name: str) -> bool:
    row = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (name,),
    ).fetchone()
    return bool(row[0]) if row else False


def main() -> int:
    if not NEON_DATABASE_URL:
        print("ERROR: NEON_DATABASE_URL is not set. Add it to your .env file.")
        return 1

    print("=" * 78)
    print("NEON SCHEMA SETUP")
    print("=" * 78)

    try:
        with psycopg.connect(_require_url()) as conn:
            with conn.transaction():
                for label, sql in _SCHEMA_STATEMENTS:
                    if label in _TABLES:
                        existed = _table_exists(conn, label)
                        conn.execute(sql)
                        if existed:
                            print(f"Table {label} already exists")
                        else:
                            print(f"Created table {label}")
                    else:
                        conn.execute(sql)
                        print(f"Ensured {label}")
    except Exception as exc:
        print(f"\nERROR: schema setup failed (rolled back): {exc}")
        return 1

    print("\nSchema setup complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
