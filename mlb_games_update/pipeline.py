"""Pipeline for mlb_games_update.

Owns the SQLAlchemy engine, the writer SQL, and the sync orchestration.

SCHEMA SYNC: the writer SQL here mirrors `propgpt-mlb/src/propgpt_mlb/ingestion/writers.py`.
When the propgpt_mlb schema changes, update both files. The marker comments below
delimit each writer for easier diffing.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy import Engine, create_engine, text

from .mlb_stats_client import MLBStatsClient

logger = logging.getLogger("mlb_games_update.pipeline")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# All tables live in this schema. Every SQL statement in this module
# uses schema-qualified table names (e.g. propgpt_mlb.games) rather than
# relying on search_path. This is required because Neon's pooled endpoint
# (`*-pooler.*`) runs PgBouncer in transaction mode, where session-level
# SET search_path doesn't reliably persist across transactions.
SCHEMA = "propgpt_mlb"


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    """Build a SQLAlchemy Engine pointed at Neon.

    Uses pool_pre_ping to handle Neon's idle-connection drops gracefully.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set — check GitHub Secrets.")

    # SQLAlchemy needs the postgresql+psycopg dialect prefix to use psycopg 3.
    # If the URL starts with postgresql:// (no driver), upgrade it.
    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Writers — SCHEMA SYNC: mirror propgpt-mlb/src/propgpt_mlb/ingestion/writers.py
# ---------------------------------------------------------------------------

def upsert_team(engine: Engine, team_data: dict[str, Any]) -> None:
    team_id = team_data["id"]
    abbr = team_data["abbreviation"]
    name = team_data["name"]

    league_full = (team_data.get("league") or {}).get("name", "")
    if "American" in league_full:
        league = "AL"
    elif "National" in league_full:
        league = "NL"
    else:
        raise ValueError(f"Could not derive league from {league_full!r} for {abbr}")

    division_full = (team_data.get("division") or {}).get("name", "")
    if "East" in division_full:
        division = "East"
    elif "Central" in division_full:
        division = "Central"
    elif "West" in division_full:
        division = "West"
    else:
        raise ValueError(f"Could not derive division from {division_full!r} for {abbr}")

    primary_park_id = (team_data.get("venue") or {}).get("id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.teams (team_id, abbr, name, league, division, primary_park_id)
                VALUES (:team_id, :abbr, :name, :league, :division, :primary_park_id)
                ON CONFLICT (team_id) DO UPDATE SET
                    abbr = EXCLUDED.abbr,
                    name = EXCLUDED.name,
                    league = EXCLUDED.league,
                    division = EXCLUDED.division,
                    primary_park_id = EXCLUDED.primary_park_id
            """),
            {
                "team_id": team_id, "abbr": abbr, "name": name,
                "league": league, "division": division,
                "primary_park_id": primary_park_id,
            },
        )


def upsert_park(engine: Engine, venue_data: dict[str, Any]) -> None:
    park_id = venue_data["id"]
    name = venue_data["name"]
    loc = venue_data.get("location") or {}
    coords = loc.get("defaultCoordinates") or {}
    latitude = coords.get("latitude")
    longitude = coords.get("longitude")
    timezone_id = (venue_data.get("timeZone") or {}).get("id")

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.parks (park_id, name, latitude, longitude, timezone)
                VALUES (:park_id, :name, :latitude, :longitude, :timezone)
                ON CONFLICT (park_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    latitude = COALESCE(EXCLUDED.latitude, {SCHEMA}.parks.latitude),
                    longitude = COALESCE(EXCLUDED.longitude, {SCHEMA}.parks.longitude),
                    timezone = COALESCE(EXCLUDED.timezone, {SCHEMA}.parks.timezone)
            """),
            {
                "park_id": park_id, "name": name,
                "latitude": latitude, "longitude": longitude,
                "timezone": timezone_id,
            },
        )


def upsert_player_stub(engine: Engine, player_id: int, full_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.players (player_id, full_name)
                VALUES (:player_id, :full_name)
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name = COALESCE(NULLIF({SCHEMA}.players.full_name, ''), EXCLUDED.full_name)
            """),
            {"player_id": player_id, "full_name": full_name},
        )


def upsert_game(engine: Engine, schedule_game: dict[str, Any]) -> None:
    game_id = schedule_game["gamePk"]
    game_date = schedule_game.get("officialDate") or schedule_game["gameDate"][:10]
    season = int(schedule_game["season"])

    teams_block = schedule_game["teams"]
    home_team_id = teams_block["home"]["team"]["id"]
    away_team_id = teams_block["away"]["team"]["id"]

    park_id = (schedule_game.get("venue") or {}).get("id")
    game_time_utc = schedule_game.get("gameDate")

    home_sp = (teams_block["home"].get("probablePitcher") or {}).get("id")
    away_sp = (teams_block["away"].get("probablePitcher") or {}).get("id")

    status = (schedule_game.get("status") or {}).get("detailedState") \
             or (schedule_game.get("status") or {}).get("abstractGameState")

    dh_flag = schedule_game.get("doubleHeader", "N")
    is_doubleheader = dh_flag != "N"
    doubleheader_game_num = schedule_game.get("gameNumber") if is_doubleheader else None

    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.games (
                    game_id, game_date, season, home_team_id, away_team_id, park_id,
                    game_time_utc, home_sp_id, away_sp_id, status,
                    is_doubleheader, doubleheader_game_num, updated_at
                )
                VALUES (
                    :game_id, :game_date, :season, :home_team_id, :away_team_id, :park_id,
                    :game_time_utc, :home_sp_id, :away_sp_id, :status,
                    :is_doubleheader, :doubleheader_game_num, NOW()
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    game_date = EXCLUDED.game_date,
                    season = EXCLUDED.season,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    park_id = COALESCE(EXCLUDED.park_id, {SCHEMA}.games.park_id),
                    game_time_utc = COALESCE(EXCLUDED.game_time_utc, {SCHEMA}.games.game_time_utc),
                    home_sp_id = COALESCE(EXCLUDED.home_sp_id, {SCHEMA}.games.home_sp_id),
                    away_sp_id = COALESCE(EXCLUDED.away_sp_id, {SCHEMA}.games.away_sp_id),
                    status = EXCLUDED.status,
                    is_doubleheader = EXCLUDED.is_doubleheader,
                    doubleheader_game_num = EXCLUDED.doubleheader_game_num,
                    updated_at = NOW()
            """),
            {
                "game_id": game_id, "game_date": game_date, "season": season,
                "home_team_id": home_team_id, "away_team_id": away_team_id,
                "park_id": park_id, "game_time_utc": game_time_utc,
                "home_sp_id": home_sp, "away_sp_id": away_sp, "status": status,
                "is_doubleheader": is_doubleheader,
                "doubleheader_game_num": doubleheader_game_num,
            },
        )


def count_rows(engine: Engine, table: str) -> int:
    """Hardcoded-table-name count helper. Only call with literals."""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{table}")).fetchone()
        return int(row.n) if row else 0


def log_row_counts(engine: Engine) -> None:
    counts = {
        "teams": count_rows(engine, "teams"),
        "parks": count_rows(engine, "parks"),
        "players": count_rows(engine, "players"),
        "games": count_rows(engine, "games"),
    }
    logger.info("Row counts: %s", counts)


# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------

def bootstrap_teams_and_parks_if_empty(engine: Engine, season: int) -> None:
    """Populate teams + parks only on a fresh DB. No-op once seeded."""
    existing = count_rows(engine, "teams")
    if existing >= 30:
        logger.info("Teams already populated (%d rows) — skipping bootstrap", existing)
        return

    logger.info("Teams table has %d rows — bootstrapping from MLB Stats API", existing)
    client = MLBStatsClient()
    teams_data = client.get_teams(season=season)
    logger.info("Fetched %d teams", len(teams_data))

    venue_ids = {(t.get("venue") or {}).get("id") for t in teams_data if (t.get("venue") or {}).get("id")}
    parks_synced = 0
    for venue_id in venue_ids:
        try:
            venue_full = client.get_venue(venue_id)
            upsert_park(engine, venue_full)
            parks_synced += 1
        except Exception as e:
            logger.warning("Failed to upsert park %s: %s", venue_id, e)

    teams_synced = 0
    for team in teams_data:
        try:
            upsert_team(engine, team)
            teams_synced += 1
        except Exception as e:
            logger.warning("Failed to upsert team %s: %s", team.get("abbreviation"), e)

    logger.info("Bootstrap complete: %d teams, %d parks", teams_synced, parks_synced)


def sync_games_for_date(engine: Engine, target_date: str) -> dict[str, int]:
    """Pull schedule + probable pitchers for a date, upsert into games."""
    client = MLBStatsClient()
    games = client.get_schedule_with_pitchers(target_date)
    logger.info("Fetched %d games for %s", len(games), target_date)

    pitcher_ids: dict[int, str] = {}
    for game in games:
        for side in ("home", "away"):
            pp = (game["teams"][side].get("probablePitcher") or {})
            if "id" in pp:
                pitcher_ids[pp["id"]] = pp.get("fullName", "")

    players_stubbed = 0
    for pid, name in pitcher_ids.items():
        try:
            upsert_player_stub(engine, pid, name or f"Player {pid}")
            players_stubbed += 1
        except Exception as e:
            logger.warning("Failed to stub player %s: %s", pid, e)

    games_synced = 0
    for game in games:
        try:
            upsert_game(engine, game)
            games_synced += 1
        except Exception as e:
            logger.warning("Failed to upsert game %s: %s", game.get("gamePk"), e)

    return {"games": games_synced, "players_stubbed": players_stubbed}
