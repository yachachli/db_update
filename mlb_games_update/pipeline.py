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

# libpq connection args. TCP keepalives let the OS detect a dead/black-holed socket in
# tens of seconds instead of blocking indefinitely (the cause of multi-hour hangs during
# a long backfill on a flaky network); connect_timeout bounds the initial dial.
# NOTE: no server-side `options` (e.g. statement_timeout) — Neon's pooled PgBouncer
# endpoint rejects startup `options`; keepalives are the real anti-hang fix regardless.
_CONNECT_ARGS = {
    "connect_timeout": 10,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3,
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    """Build a SQLAlchemy Engine pointed at Neon.

    Uses pool_pre_ping + short pool_recycle + TCP keepalives to survive Neon's
    idle-connection drops and flaky networks during long backfills.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set — check GitHub Secrets.")

    # SQLAlchemy needs the postgresql+psycopg dialect prefix to use psycopg 3.
    # If the URL starts with postgresql:// (no driver), upgrade it.
    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(url, pool_pre_ping=True, pool_recycle=300,
                         connect_args=_CONNECT_ARGS)


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


def upsert_player_full(engine: Engine, person: dict[str, Any]) -> None:
    """Enrich an existing player row with base /people fields, never clobbering non-NULLs.

    COALESCE(EXCLUDED.x, existing) keeps any value already present — the schedule sync
    only stubs name, so this fills handedness/position/dates without overwriting data a
    later results ingestion may have set.
    """
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.players (
                    player_id, full_name, primary_position, throws, bats,
                    birth_date, mlb_debut_date
                )
                VALUES (
                    :player_id, :full_name, :primary_position, :throws, :bats,
                    :birth_date, :mlb_debut_date
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    primary_position = COALESCE(EXCLUDED.primary_position, {SCHEMA}.players.primary_position),
                    throws = COALESCE(EXCLUDED.throws, {SCHEMA}.players.throws),
                    bats = COALESCE(EXCLUDED.bats, {SCHEMA}.players.bats),
                    birth_date = COALESCE(EXCLUDED.birth_date, {SCHEMA}.players.birth_date),
                    mlb_debut_date = COALESCE(EXCLUDED.mlb_debut_date, {SCHEMA}.players.mlb_debut_date)
            """),
            {
                "player_id": person["id"],
                "full_name": person["fullName"],
                "primary_position": (person.get("primaryPosition") or {}).get("abbreviation"),
                "throws": (person.get("pitchHand") or {}).get("code"),
                "bats": (person.get("batSide") or {}).get("code"),
                "birth_date": person.get("birthDate"),
                "mlb_debut_date": person.get("mlbDebutDate"),
            },
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


def load_known_park_ids(engine: Engine) -> set[int]:
    """All park_ids currently in the parks table — used to detect neutral-site venues
    that need stubbing before a game referencing them can be inserted (games.park_id FK)."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT park_id FROM {SCHEMA}.parks")).fetchall()
        return {r.park_id for r in rows}


def ensure_park(engine: Engine, client: MLBStatsClient, venue: dict[str, Any],
                known_park_ids: set[int]) -> bool:
    """Guarantee a game's venue exists in parks so the games.park_id FK is satisfiable.

    2024 includes neutral-site games (Seoul, London, Mexico City, Rickwood, Williamsport)
    whose venues aren't in the seeded parks table. Rather than let the FK violation skip
    the game, we stub the venue from /venues/{id} (id, name, coords, timezone; no park
    factors — downstream treats factor-less parks as league-average). Returns True if a
    new park was stubbed.
    """
    venue_id = venue.get("id")
    if venue_id is None or venue_id in known_park_ids:
        return False

    try:
        venue_full = client.get_venue(venue_id)
    except Exception as e:
        # Fall back to the minimal name from the schedule so the game still lands.
        logger.warning("Could not fetch /venues/%s (%s) for stub: %s — stubbing name only",
                       venue_id, venue.get("name"), e)
        venue_full = {"id": venue_id, "name": venue.get("name") or f"Venue {venue_id}"}

    upsert_park(engine, venue_full)
    known_park_ids.add(venue_id)
    logger.info("Stubbed neutral-site venue into parks: id=%s name=%r",
                venue_id, venue_full.get("name"))
    return True


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

    # Regular season only ('R'). Spring Training ('S'), exhibitions ('E'), All-Star ('A'),
    # and postseason ('P'/'D'/'L'/'W'/'F') are excluded from training + daily prediction.
    reg_games = [g for g in games if g.get("gameType") == "R"]
    filtered_non_reg = len(games) - len(reg_games)
    if filtered_non_reg:
        types = sorted({g.get("gameType") for g in games if g.get("gameType") != "R"})
        logger.info("Filtered %d non-regular-season game(s) on %s (gameType in %s)",
                    filtered_non_reg, target_date, types)
    games = reg_games

    # Neutral-site venues (Seoul/London/etc.) must exist in parks before we upsert games.
    known_park_ids = load_known_park_ids(engine)
    venues_stubbed = 0
    for game in games:
        if ensure_park(engine, client, game.get("venue") or {}, known_park_ids):
            venues_stubbed += 1

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

    probables_enriched, enrich_failures = enrich_probable_pitchers(engine, target_date)

    logger.info(
        "Games sync for %s — games: %d, filtered non-reg: %d, venues stubbed: %d, "
        "players stubbed: %d, probable pitchers enriched: %d (failures: %d)",
        target_date, games_synced, filtered_non_reg, venues_stubbed, players_stubbed,
        probables_enriched, enrich_failures,
    )

    return {
        "games": games_synced,
        "filtered_non_reg": filtered_non_reg,
        "venues_stubbed": venues_stubbed,
        "players_stubbed": players_stubbed,
        "probables_enriched": probables_enriched,
        "probables_enrich_failures": enrich_failures,
    }


def get_probables_needing_enrichment(engine: Engine, target_date: str) -> list[int]:
    """Player IDs listed as a probable SP (home or away) on target_date whose `throws`
    is still NULL. The NULL filter makes re-runs / already-enriched dates a no-op."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT DISTINCT p.player_id
                FROM {SCHEMA}.players p
                JOIN {SCHEMA}.games g
                  ON p.player_id IN (g.home_sp_id, g.away_sp_id)
                WHERE g.game_date = :target_date
                  AND p.throws IS NULL
            """),
            {"target_date": target_date},
        ).fetchall()
        return [r.player_id for r in rows]


def enrich_probable_pitchers(engine: Engine, target_date: str) -> tuple[int, int]:
    """Proactively hydrate handedness (and other base person fields) for the date's
    probable starters, so the model has `throws` before first pitch rather than waiting
    for results ingestion. Returns (enriched, failures).

    Typically 0–15 lookups/day; serial and polite, reusing the client's retry/backoff.
    A failed /people lookup logs a warning and is skipped — it never fails the run.
    """
    pitcher_ids = get_probables_needing_enrichment(engine, target_date)
    if not pitcher_ids:
        return 0, 0

    logger.info("%d probable pitcher(s) on %s need handedness enrichment",
                len(pitcher_ids), target_date)
    client = MLBStatsClient()
    enriched = 0
    failures = 0
    for pid in pitcher_ids:
        try:
            person = client.get_person(pid)
            upsert_player_full(engine, person)
            enriched += 1
        except Exception as e:
            failures += 1
            logger.warning("Failed to enrich probable pitcher %s: %s", pid, e)
    return enriched, failures
