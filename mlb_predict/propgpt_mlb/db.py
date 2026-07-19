"""Neon Postgres connection layer.

This DB is shared with the NBA model project. All MLB tables live in the
`propgpt_mlb` schema. We use schema-qualified table names everywhere
(e.g. `propgpt_mlb.games`) rather than relying on `search_path`, because
Neon's pooled connection endpoint uses PgBouncer in transaction-mode pooling,
which does NOT preserve session state across transactions.

DATABASE_URL is read from .env. Includes simple retry on transient
connection failures.
"""
from __future__ import annotations

import logging
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"

SCHEMA = "propgpt_mlb"
"""Postgres schema for all MLB tables.

Always qualify table names with this constant in raw SQL. Do NOT rely on
search_path — Neon's pooled endpoint runs PgBouncer in transaction mode,
which doesn't preserve session state across transactions.

Example:
    cur.execute(f"SELECT * FROM {SCHEMA}.games WHERE game_date = %s", (d,))
"""

_MAX_RETRIES = 3
_RETRY_BACKOFF_SEC = 1.5


class DatabaseUnavailable(RuntimeError):
    """Raised when Neon cannot be reached after retries."""


def _require_url() -> str:
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Open .env and paste your Neon connection string."
        )
    return DATABASE_URL


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    """Context-managed Neon connection with retry on transient connection failures.

    Does NOT set search_path — all SQL must use schema-qualified table names
    (e.g. propgpt_mlb.games) because the pooled endpoint doesn't preserve
    session state across transactions.
    """
    url = _require_url()
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            with psycopg.connect(url, row_factory=dict_row) as conn:
                yield conn
                return
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            last_exc = e
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF_SEC * attempt
                logger.warning(
                    "DB connection attempt %d/%d failed: %s — retrying in %.1fs",
                    attempt, _MAX_RETRIES, e, wait,
                )
                time.sleep(wait)
            else:
                break
    raise DatabaseUnavailable(
        f"Could not connect to Neon after {_MAX_RETRIES} attempts: {last_exc}"
    ) from last_exc


# psycopg3 still parses `%` placeholders when params are passed (even an empty
# tuple), so SQL containing literal `%` (e.g. `LIKE 'mlb_%'`) breaks. We pass
# params through only when explicitly provided.
def _execute(cur: psycopg.Cursor, sql: str, params: Iterable[Any] | None) -> None:
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)


def fetch_one(sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    with get_connection() as conn, conn.cursor() as cur:
        _execute(cur, sql, params)
        return cur.fetchone()


def fetch_all(sql: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    with get_connection() as conn, conn.cursor() as cur:
        _execute(cur, sql, params)
        return list(cur.fetchall())


def execute(sql: str, params: Iterable[Any] | None = None) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        _execute(cur, sql, params)
        return cur.rowcount


def execute_many(sql: str, param_list: Iterable[Iterable[Any]]) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.executemany(sql, list(param_list))
        return cur.rowcount


# Note: simple ;-split — does not handle dollar-quoted function bodies.
# Fine for plain DDL. If we ever add stored procedures / triggers, switch
# to a real SQL parser.
def _split_sql_statements(sql_text: str) -> list[str]:
    """Split a SQL file into individual statements. Psycopg 3 requires one
    statement per execute() call."""
    cleaned = re.sub(r'--[^\n]*', '', sql_text)
    parts = [s.strip() for s in cleaned.split(';')]
    return [s for s in parts if s]


def run_migrations() -> list[str]:
    """Apply any unrun migrations from migrations/ in filename order.

    Returns the list of newly-applied migration versions.
    """
    if not MIGRATIONS_DIR.is_dir():
        raise RuntimeError(f"Migrations directory not found: {MIGRATIONS_DIR}")

    # Bootstrap: ensure schema + tracking table exist before applying any migrations.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    applied = {
        row["version"]
        for row in fetch_all(f"SELECT version FROM {SCHEMA}.schema_migrations")
    }
    sql_files = sorted(p for p in MIGRATIONS_DIR.glob("*.sql"))
    if not sql_files:
        logger.warning("No migration files found in %s", MIGRATIONS_DIR)
        return []

    newly_applied: list[str] = []
    for path in sql_files:
        version = path.stem
        if version in applied:
            continue
        logger.info("Applying migration: %s", version)
        sql_text = path.read_text(encoding="utf-8")
        statements = _split_sql_statements(sql_text)
        with get_connection() as conn, conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
            cur.execute(
                f"INSERT INTO {SCHEMA}.schema_migrations (version) "
                "VALUES (%s) ON CONFLICT DO NOTHING",
                (version,),
            )
        newly_applied.append(version)
        logger.info("Applied %s", version)
    return newly_applied
