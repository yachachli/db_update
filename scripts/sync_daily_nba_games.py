import asyncio
import datetime as dt
import json
import os
from zoneinfo import ZoneInfo
from collections.abc import Sequence
from typing import Any

import asyncpg
import httpx


TEAM_ABV_ALIASES = {
    "GS": "GSW",
    "NO": "NOP",
    "NY": "NYK",
    "PHO": "PHX",
    "SA": "SAS",
}


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.nba_game_predictions (
    id BIGSERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    predicted_winner TEXT,
    predicted_spread NUMERIC(8, 3),
    vegas_spread NUMERIC(5, 1),
    vegas_home_moneyline INTEGER,
    vegas_away_moneyline INTEGER,
    home_win_prob NUMERIC(8, 6),
    away_win_prob NUMERIC(8, 6),
    confidence TEXT,
    home_adj_em NUMERIC(10, 4),
    away_adj_em NUMERIC(10, 4),
    home_offensive_em NUMERIC(10, 4),
    home_defensive_em NUMERIC(10, 4),
    away_offensive_em NUMERIC(10, 4),
    away_defensive_em NUMERIC(10, 4),
    matchup_pace NUMERIC(10, 4),
    short_answer TEXT,
    long_answer TEXT,
    head_to_head TEXT,
    head_to_head_games JSONB NOT NULL DEFAULT '[]'::jsonb,
    home_last_10_games JSONB NOT NULL DEFAULT '[]'::jsonb,
    away_last_10_games JSONB NOT NULL DEFAULT '[]'::jsonb,
    factors JSONB NOT NULL DEFAULT '[]'::jsonb,
    home_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    away_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    game_payload JSONB NOT NULL,
    source_payload JSONB NOT NULL,
    source_endpoint TEXT NOT NULL,
    github_run_id TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ux_nba_game_predictions UNIQUE (prediction_date, home_team, away_team),
    CONSTRAINT ck_nba_game_predictions_teams_diff CHECK (home_team <> away_team)
);

CREATE INDEX IF NOT EXISTS idx_nba_game_predictions_date
ON public.nba_game_predictions (prediction_date DESC);
"""

ALTER_TABLE_SQL = """
ALTER TABLE public.nba_game_predictions
    ADD COLUMN IF NOT EXISTS vegas_spread NUMERIC(5, 1),
    ADD COLUMN IF NOT EXISTS vegas_home_moneyline INTEGER,
    ADD COLUMN IF NOT EXISTS vegas_away_moneyline INTEGER;
"""

FETCH_VEGAS_ODDS_SQL = """
SELECT DISTINCT ON (game_date, home_team, away_team)
    game_date,
    home_team,
    away_team,
    spread,
    home_moneyline,
    away_moneyline,
    source
FROM public.nba_historical_odds
WHERE game_date BETWEEN $1 AND $2
ORDER BY
    game_date,
    home_team,
    away_team,
    CASE WHEN source = 'consensus' THEN 0 ELSE 1 END,
    captured_at DESC,
    id DESC;
"""

FETCH_RECENT_PREDICTIONS_SQL = """
SELECT id, prediction_date, home_team, away_team
FROM public.nba_game_predictions
WHERE prediction_date BETWEEN $1 AND $2
ORDER BY prediction_date DESC, id DESC;
"""

UPDATE_VEGAS_ODDS_SQL = """
UPDATE public.nba_game_predictions
SET
    vegas_spread = $2,
    vegas_home_moneyline = $3,
    vegas_away_moneyline = $4,
    updated_at = NOW()
WHERE id = $1;
"""

UPSERT_SQL = """
INSERT INTO public.nba_game_predictions (
    prediction_date,
    home_team,
    away_team,
    predicted_winner,
    predicted_spread,
    vegas_spread,
    vegas_home_moneyline,
    vegas_away_moneyline,
    home_win_prob,
    away_win_prob,
    confidence,
    home_adj_em,
    away_adj_em,
    home_offensive_em,
    home_defensive_em,
    away_offensive_em,
    away_defensive_em,
    matchup_pace,
    short_answer,
    long_answer,
    head_to_head,
    head_to_head_games,
    home_last_10_games,
    away_last_10_games,
    factors,
    home_context,
    away_context,
    game_payload,
    source_payload,
    source_endpoint,
    github_run_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
    $22::jsonb, $23::jsonb, $24::jsonb, $25::jsonb, $26::jsonb,
    $27::jsonb, $28::jsonb, $29::jsonb, $30, $31
)
ON CONFLICT (prediction_date, home_team, away_team)
DO UPDATE SET
    predicted_winner = EXCLUDED.predicted_winner,
    predicted_spread = EXCLUDED.predicted_spread,
    vegas_spread = COALESCE(EXCLUDED.vegas_spread, nba_game_predictions.vegas_spread),
    vegas_home_moneyline = COALESCE(EXCLUDED.vegas_home_moneyline, nba_game_predictions.vegas_home_moneyline),
    vegas_away_moneyline = COALESCE(EXCLUDED.vegas_away_moneyline, nba_game_predictions.vegas_away_moneyline),
    home_win_prob = EXCLUDED.home_win_prob,
    away_win_prob = EXCLUDED.away_win_prob,
    confidence = EXCLUDED.confidence,
    home_adj_em = EXCLUDED.home_adj_em,
    away_adj_em = EXCLUDED.away_adj_em,
    home_offensive_em = EXCLUDED.home_offensive_em,
    home_defensive_em = EXCLUDED.home_defensive_em,
    away_offensive_em = EXCLUDED.away_offensive_em,
    away_defensive_em = EXCLUDED.away_defensive_em,
    matchup_pace = EXCLUDED.matchup_pace,
    short_answer = EXCLUDED.short_answer,
    long_answer = EXCLUDED.long_answer,
    head_to_head = EXCLUDED.head_to_head,
    head_to_head_games = EXCLUDED.head_to_head_games,
    home_last_10_games = EXCLUDED.home_last_10_games,
    away_last_10_games = EXCLUDED.away_last_10_games,
    factors = EXCLUDED.factors,
    home_context = EXCLUDED.home_context,
    away_context = EXCLUDED.away_context,
    game_payload = EXCLUDED.game_payload,
    source_payload = EXCLUDED.source_payload,
    source_endpoint = EXCLUDED.source_endpoint,
    github_run_id = EXCLUDED.github_run_id,
    updated_at = NOW();
"""


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_prediction_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid response date format: {value!r}. Expected YYYYMMDD."
        ) from exc


async def fetch_predictions(
    endpoint: str, bearer_token: str | None = None, *, date: str | None = None
) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    timeout = httpx.Timeout(timeout=300.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if date:
            response = await client.post(
                endpoint, headers=headers, json={"date": date}
            )
        else:
            response = await client.get(endpoint, headers=headers)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Predictor response was not a JSON object.")
    return payload


def row_values(
    prediction_date: dt.date,
    prediction: dict[str, Any],
    vegas_odds: dict[str, Any] | None,
    source_payload: dict[str, Any],
    source_endpoint: str,
    github_run_id: str | None,
) -> tuple[Any, ...]:
    enriched_prediction = dict(prediction)
    if vegas_odds:
        enriched_prediction["vegas_spread"] = vegas_odds.get("spread")
        enriched_prediction["vegas_home_moneyline"] = vegas_odds.get("home_moneyline")
        enriched_prediction["vegas_away_moneyline"] = vegas_odds.get("away_moneyline")

    return (
        prediction_date,
        prediction.get("home_team"),
        prediction.get("away_team"),
        prediction.get("predicted_winner"),
        prediction.get("predicted_spread"),
        vegas_odds.get("spread") if vegas_odds else None,
        vegas_odds.get("home_moneyline") if vegas_odds else None,
        vegas_odds.get("away_moneyline") if vegas_odds else None,
        prediction.get("home_win_prob"),
        prediction.get("away_win_prob"),
        prediction.get("confidence"),
        prediction.get("home_adj_em"),
        prediction.get("away_adj_em"),
        prediction.get("home_offensive_em"),
        prediction.get("home_defensive_em"),
        prediction.get("away_offensive_em"),
        prediction.get("away_defensive_em"),
        prediction.get("matchup_pace"),
        prediction.get("short_answer"),
        prediction.get("long_answer"),
        prediction.get("head_to_head"),
        json.dumps(prediction.get("head_to_head_games", [])),
        json.dumps(prediction.get("home_last_10_games", [])),
        json.dumps(prediction.get("away_last_10_games", [])),
        json.dumps(prediction.get("factors", [])),
        json.dumps(prediction.get("home_context", {})),
        json.dumps(prediction.get("away_context", {})),
        json.dumps(enriched_prediction),
        json.dumps(source_payload),
        source_endpoint,
        github_run_id,
    )


def canonical_team_abv(team: str | None) -> str | None:
    if not team:
        return team
    return TEAM_ABV_ALIASES.get(team, team)


def build_matchup_key(
    game_date: dt.date, home_team: str | None, away_team: str | None
) -> tuple[dt.date, str | None, str | None]:
    return (
        game_date,
        canonical_team_abv(home_team),
        canonical_team_abv(away_team),
    )


async def fetch_vegas_odds(
    conn: asyncpg.Connection,
    start_date: dt.date,
    end_date: dt.date,
) -> dict[tuple[dt.date, str | None, str | None], dict[str, Any]]:
    try:
        rows = await conn.fetch(FETCH_VEGAS_ODDS_SQL, start_date, end_date)
    except Exception as exc:
        print(
            "Warning: failed to load Vegas odds from public.nba_historical_odds "
            f"for {start_date.isoformat()} to {end_date.isoformat()}: {exc}"
        )
        return {}

    odds_by_matchup: dict[tuple[dt.date, str | None, str | None], dict[str, Any]] = {}
    for row in rows:
        odds_by_matchup[
            build_matchup_key(row["game_date"], row["home_team"], row["away_team"])
        ] = {
            "spread": float(row["spread"]) if row["spread"] is not None else None,
            "home_moneyline": int(row["home_moneyline"])
            if row["home_moneyline"] is not None
            else None,
            "away_moneyline": int(row["away_moneyline"])
            if row["away_moneyline"] is not None
            else None,
            "source": row["source"],
        }
    return odds_by_matchup


async def backfill_recent_predictions(
    conn: asyncpg.Connection,
    start_date: dt.date,
    end_date: dt.date,
    odds_by_matchup: dict[tuple[dt.date, str | None, str | None], dict[str, Any]],
) -> int:
    rows = await conn.fetch(FETCH_RECENT_PREDICTIONS_SQL, start_date, end_date)
    updates: list[tuple[Any, ...]] = []
    for row in rows:
        vegas_odds = odds_by_matchup.get(
            build_matchup_key(
                row["prediction_date"],
                row["home_team"],
                row["away_team"],
            )
        )
        if not vegas_odds:
            continue
        updates.append(
            (
                row["id"],
                vegas_odds.get("spread"),
                vegas_odds.get("home_moneyline"),
                vegas_odds.get("away_moneyline"),
            )
        )

    if updates:
        await conn.executemany(UPDATE_VEGAS_ODDS_SQL, updates)
    return len(updates)


async def get_connection() -> asyncpg.Connection:
    database_url = os.getenv("NEON_DATABASE_URL") or os.getenv("DB_URL")
    if database_url:
        return await asyncpg.connect(dsn=database_url)

    db_name = require_env("DB_NAME")
    db_user = require_env("DB_USER")
    db_pass = require_env("DB_PASS")
    db_host = require_env("DB_HOST")
    return await asyncpg.connect(
        database=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        ssl="require",
    )


async def sync_predictions_for_date(
    conn: asyncpg.Connection,
    endpoint: str,
    bearer_token: str | None,
    github_run_id: str | None,
    target_date: str,
) -> None:
    """Fetch predictions for a specific date and upsert into the database."""
    print(f"\n--- Syncing predictions for {target_date} ---")
    payload = await fetch_predictions(endpoint, bearer_token, date=target_date)
    date_raw = payload.get("date")
    if not isinstance(date_raw, str):
        raise RuntimeError("Predictor response is missing string field 'date'.")

    predictions_raw = payload.get("predictions")
    if not isinstance(predictions_raw, list):
        raise RuntimeError("Predictor response is missing list field 'predictions'.")

    if date_raw != target_date:
        raise RuntimeError("Prediction endpoint returned the wrong date")
    prediction_date = parse_prediction_date(date_raw)
    predictions: list[dict[str, Any]] = [
        p for p in predictions_raw if isinstance(p, dict)
    ]

    season_start=prediction_date.year if prediction_date.month>=10 else prediction_date.year-1
    expected_season=f"{season_start}-{(season_start+1)%100:02d}"
    for prediction in predictions:
        context=prediction.get('model_context') or {}
        if context.get('season')!=expected_season or (context.get('data_freshness') or {}).get('status')!='complete':
            raise RuntimeError('Prediction lacks verified current-season input provenance')

    if not predictions:
        print(f"No games found for {prediction_date.isoformat()}, skipping.")
        return

    print(
        f"Fetched {len(predictions)} predictions for {prediction_date.isoformat()} "
        f"from {endpoint}"
    )

    backfill_start = prediction_date - dt.timedelta(days=3)
    backfill_end = prediction_date

    async with conn.transaction():
        odds_by_matchup = await fetch_vegas_odds(
            conn, start_date=backfill_start, end_date=backfill_end,
        )
        existing = await conn.fetch('SELECT home_team,away_team,vegas_spread,vegas_home_moneyline,vegas_away_moneyline FROM nba_game_predictions WHERE prediction_date=$1',prediction_date)
        for r in existing:
            key=build_matchup_key(prediction_date,r['home_team'],r['away_team'])
            if key not in odds_by_matchup:
                odds_by_matchup[key]={'spread':float(r['vegas_spread']) if r['vegas_spread'] is not None else None,
                    'home_moneyline':r['vegas_home_moneyline'],'away_moneyline':r['vegas_away_moneyline']}
        rows: Sequence[tuple[Any, ...]] = [
            row_values(
                prediction_date=prediction_date,
                prediction=prediction,
                vegas_odds=odds_by_matchup.get(
                    build_matchup_key(
                        prediction_date,
                        prediction.get("home_team"),
                        prediction.get("away_team"),
                    )
                ),
                source_payload=payload,
                source_endpoint=endpoint,
                github_run_id=github_run_id,
            )
            for prediction in predictions
        ]
        await conn.executemany(UPSERT_SQL, rows)
        backfilled_rows = await backfill_recent_predictions(
            conn,
            start_date=backfill_start,
            end_date=backfill_end,
            odds_by_matchup=odds_by_matchup,
        )

    odds_matches = sum(
        1
        for prediction in predictions
        if build_matchup_key(
            prediction_date,
            prediction.get("home_team"),
            prediction.get("away_team"),
        )
        in odds_by_matchup
    )
    print(f"Upserted {len(predictions)} rows into public.nba_game_predictions.")
    print(f"Loaded {len(odds_by_matchup)} Vegas odds rows for backfill window.")
    print(f"Matched Vegas odds for {odds_matches} of {len(predictions)} current predictions.")
    print(f"Backfilled Vegas odds for {backfilled_rows} recent prediction rows.")


async def main() -> None:
    endpoint = require_env("NBA_GAME_PREDICTOR_URL")
    bearer_token = os.getenv("NBA_GAME_PREDICTOR_BEARER_TOKEN")
    github_run_id = os.getenv("GITHUB_RUN_ID")

    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    target_dates = [
        today.strftime("%Y%m%d"),
        (today + dt.timedelta(days=1)).strftime("%Y%m%d"),
        (today + dt.timedelta(days=2)).strftime("%Y%m%d"),
    ]

    conn = await get_connection()
    try:
        await conn.execute(CREATE_TABLE_SQL)
        await conn.execute(ALTER_TABLE_SQL)
        failures = []
        for target_date in target_dates:
            try:
                await sync_predictions_for_date(
                    conn, endpoint, bearer_token, github_run_id, target_date
                )
            except Exception as exc:
                failures.append(target_date)
                print(f"ERROR: failed to sync predictions for {target_date}: {exc}")
        if failures:
            raise RuntimeError(f"NBA prediction publication failed for {failures}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())

