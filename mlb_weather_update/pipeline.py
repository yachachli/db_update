"""Pipeline for mlb_weather_update.

Populates propgpt_mlb.weather_observations with a game-time forecast for every game on
the slate, using park coordinates. Fixed-dome games get a row with the dome flag set and
weather fields NULL; retractable-roof and open-air games get the real forecast for the
hour closest to first pitch.

Unlike odds, weather is not a time series — one row per game, ON CONFLICT DO UPDATE, so
later runs overwrite with a fresher forecast.

SCHEMA: every table reference is schema-qualified (Neon pooled endpoint = PgBouncer
transaction mode, no persistent search_path).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, time as dtime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, create_engine, text

from .open_meteo_client import OpenMeteoClient

SCHEMA = "propgpt_mlb"
SOURCE = "open-meteo"

logger = logging.getLogger("mlb_weather_update.pipeline")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# MLB slates bucket by US/Eastern calendar date (same convention as mlb_odds_update).
ET_ZONE = ZoneInfo("America/New_York")

# Fallback first pitch when a game has no scheduled start: 7pm local park time.
FALLBACK_LOCAL_HOUR = 19


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    """SQLAlchemy Engine pointed at Neon. No search_path set — all SQL is schema-qualified."""
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set — check GitHub Secrets.")

    if url.startswith("postgresql://") and "+" not in url.split("://", 1)[0]:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(url, pool_pre_ping=True)


def default_target_date() -> date:
    """Today's date in US/Eastern."""
    return datetime.now(ET_ZONE).date()


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def get_games_with_parks(engine: Engine, target_date: date) -> list[dict[str, Any]]:
    """Games on target_date joined to their park's geo + dome/roof metadata."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT
                    g.game_id, g.game_time_utc,
                    p.park_id, p.name AS park_name,
                    p.latitude, p.longitude, p.timezone,
                    p.is_dome, p.has_retractable_roof
                FROM {SCHEMA}.games g
                LEFT JOIN {SCHEMA}.parks p ON g.park_id = p.park_id
                WHERE g.game_date = :d
                ORDER BY g.game_time_utc NULLS LAST, g.game_id
            """),
            {"d": target_date},
        ).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def resolve_first_pitch_utc(game: dict[str, Any]) -> tuple[datetime, bool]:
    """Return (first_pitch_utc, was_fallback).

    Uses the scheduled game_time_utc when present; otherwise falls back to 7pm local
    park time (park.timezone) converted to UTC.
    """
    start = game.get("game_time_utc")
    if start is not None:
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        return start.astimezone(timezone.utc), False

    tz_name = game.get("timezone") or "America/New_York"
    local = datetime.combine(default_target_date(), dtime(hour=FALLBACK_LOCAL_HOUR), ZoneInfo(tz_name))
    return local.astimezone(timezone.utc), True


def select_forecast_hour(hourly: dict[str, Any], target_utc: datetime) -> int | None:
    """Index of the hourly bucket closest to target_utc. None if no times present."""
    times: list[str] = hourly.get("time", []) or []
    best_idx: int | None = None
    best_delta: float | None = None
    for i, t in enumerate(times):
        dt = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        delta = abs((dt - target_utc).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_idx = i
    return best_idx


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Writer — schema-qualified, game-level idempotent upsert
# ---------------------------------------------------------------------------

def upsert_weather(
    engine: Engine,
    *,
    game_id: int,
    observed_for_time: datetime | None,
    is_dome_game: bool,
    temp_f: float | None = None,
    feels_like_f: float | None = None,
    wind_mph: float | None = None,
    wind_dir_deg: float | None = None,
    precip_pct: float | None = None,
    humidity_pct: float | None = None,
    cloud_cover_pct: float | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {SCHEMA}.weather_observations (
                    game_id, observed_for_time, pulled_at, source,
                    temp_f, feels_like_f, wind_mph, wind_dir_deg,
                    precip_pct, humidity_pct, cloud_cover_pct, is_dome_game
                )
                VALUES (
                    :game_id, :observed_for_time, NOW(), :source,
                    :temp_f, :feels_like_f, :wind_mph, :wind_dir_deg,
                    :precip_pct, :humidity_pct, :cloud_cover_pct, :is_dome_game
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    observed_for_time = EXCLUDED.observed_for_time,
                    pulled_at = NOW(),
                    source = EXCLUDED.source,
                    temp_f = EXCLUDED.temp_f,
                    feels_like_f = EXCLUDED.feels_like_f,
                    wind_mph = EXCLUDED.wind_mph,
                    wind_dir_deg = EXCLUDED.wind_dir_deg,
                    precip_pct = EXCLUDED.precip_pct,
                    humidity_pct = EXCLUDED.humidity_pct,
                    cloud_cover_pct = EXCLUDED.cloud_cover_pct,
                    is_dome_game = EXCLUDED.is_dome_game
            """),
            {
                "game_id": game_id,
                "observed_for_time": observed_for_time,
                "source": SOURCE,
                "temp_f": temp_f,
                "feels_like_f": feels_like_f,
                "wind_mph": wind_mph,
                "wind_dir_deg": wind_dir_deg,
                "precip_pct": precip_pct,
                "humidity_pct": humidity_pct,
                "cloud_cover_pct": cloud_cover_pct,
                "is_dome_game": is_dome_game,
            },
        )


def count_rows(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) AS n FROM {SCHEMA}.{table}")).fetchone()
        return int(row.n) if row else 0


def log_row_counts(engine: Engine) -> None:
    logger.info("Row counts: %s", {"weather_observations": count_rows(engine, "weather_observations")})


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def sync_weather_for_date(engine: Engine, target_date: date) -> dict[str, int]:
    """Upsert a game-time forecast (or dome row) for every game on target_date."""
    client = OpenMeteoClient()
    games = get_games_with_parks(engine, target_date)
    logger.info("Loaded %d games for %s", len(games), target_date)

    games_processed = 0
    forecasts_fetched = 0
    dome_rows = 0
    retractable_rows = 0
    failures = 0

    for game in games:
        game_id = game["game_id"]
        try:
            # Fixed dome → dome row, no forecast (weather features go neutral downstream).
            if game.get("is_dome"):
                upsert_weather(engine, game_id=game_id, observed_for_time=None, is_dome_game=True)
                dome_rows += 1
                games_processed += 1
                continue

            lat = _num(game.get("latitude"))
            lon = _num(game.get("longitude"))
            if lat is None or lon is None:
                logger.warning("Game %s: park %s has no coordinates — skipping",
                               game_id, game.get("park_name"))
                failures += 1
                continue

            first_pitch_utc, was_fallback = resolve_first_pitch_utc(game)
            if was_fallback:
                logger.warning("Game %s: no scheduled start — using 7pm local park time (%s UTC)",
                               game_id, first_pitch_utc.isoformat())

            fc_date = first_pitch_utc.date().isoformat()
            payload = client.get_hourly(lat, lon, start_date=fc_date, end_date=fc_date)
            hourly = payload.get("hourly", {})
            idx = select_forecast_hour(hourly, first_pitch_utc)
            if idx is None:
                logger.warning("Game %s: no hourly forecast returned — skipping", game_id)
                failures += 1
                continue

            def at(var: str) -> Any:
                arr = hourly.get(var) or []
                return arr[idx] if idx < len(arr) else None

            observed_iso = (hourly.get("time") or [])[idx]
            observed_for_time = datetime.fromisoformat(observed_iso).replace(tzinfo=timezone.utc)

            upsert_weather(
                engine,
                game_id=game_id,
                observed_for_time=observed_for_time,
                is_dome_game=False,  # retractable roof status at game time is unknown; store real weather
                temp_f=_num(at("temperature_2m")),
                feels_like_f=_num(at("apparent_temperature")),
                wind_mph=_num(at("wind_speed_10m")),
                wind_dir_deg=_num(at("wind_direction_10m")),
                precip_pct=_num(at("precipitation_probability")),
                humidity_pct=_num(at("relative_humidity_2m")),
                cloud_cover_pct=_num(at("cloud_cover")),
            )
            forecasts_fetched += 1
            games_processed += 1
            if game.get("has_retractable_roof"):
                retractable_rows += 1

        except Exception as e:
            failures += 1
            logger.warning("Failed weather for game %s (%s): %s", game_id, game.get("park_name"), e)

    logger.info(
        "Weather for %s — games processed: %d, forecasts fetched: %d "
        "(retractable-roof: %d), dome rows: %d, failures: %d",
        target_date, games_processed, forecasts_fetched, retractable_rows, dome_rows, failures,
    )

    return {
        "games_processed": games_processed,
        "forecasts_fetched": forecasts_fetched,
        "retractable_rows": retractable_rows,
        "dome_rows": dome_rows,
        "failures": failures,
    }
