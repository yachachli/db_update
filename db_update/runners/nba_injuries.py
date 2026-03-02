"""Standalone NBA injury updater.

Only makes 1 API call (getNBAInjuryList) so it won't exhaust
the RapidAPI quota like the full NBA update does.
"""

import functools
import json
import os
from datetime import date
from typing import Any

import httpx

from db_update.db_pool import DBConnection, DBPool
from db_update.logger import logger

RAPIDAPI_HOST = "tank01-fantasy-stats.p.rapidapi.com"


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    r = await client.get(
        url,
        headers={
            "x-rapidapi-host": RAPIDAPI_HOST,
            "x-rapidapi-key": client.headers.get("x-rapidapi-key", ""),
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


async def run(pool: DBPool):
    logger.info("Starting NBA injury update")

    api_key = os.environ.get("RAPIDAPI_KEY", "")
    if not api_key:
        logger.error("RAPIDAPI_KEY not set — skipping injury update")
        return

    async with httpx.AsyncClient(headers={"x-rapidapi-key": api_key}) as client:
        url = f"https://{RAPIDAPI_HOST}/getNBAInjuryList"
        logger.info(f"Fetching {url}")
        data = await _fetch_json(client, url)

        injury_list: list[dict[str, Any]] = (
            data.get("body", []) if data.get("statusCode") == 200 else []
        )
        logger.info(f"Got {len(injury_list)} injury records from Tank01")

        if not injury_list:
            logger.warning("No injury data returned — clearing all injuries")
            async with pool.acquire() as conn:
                result = await conn.execute(
                    "UPDATE nba_players SET injury = NULL WHERE injury IS NOT NULL"
                )
                logger.info(f"Cleared injuries: {result}")
            return

        def is_current(injury: dict[str, Any]) -> bool:
            today = date.today().strftime("%Y%m%d")
            ret = injury.get("injReturnDate")
            return (not ret) or (ret >= today)

        # Pick most recent injury per player
        latest: dict[int, dict[str, Any]] = {}
        for inj in injury_list:
            if not is_current(inj):
                continue
            pid_raw = inj.get("playerID")
            if not pid_raw:
                continue
            try:
                pid = int(pid_raw)
            except (ValueError, TypeError):
                logger.warning(f"Skipping injury with invalid playerID: {pid_raw}")
                continue
            prev = latest.get(pid)
            if prev is None or (inj.get("injDate") or "") > (prev.get("injDate") or ""):
                latest[pid] = inj

        logger.info(f"Found {len(latest)} players with current injuries")

        # Upsert injuries
        players_with_injury = list(latest.keys())
        async with pool.acquire() as conn:
            for pid, inj in latest.items():
                await conn.execute(
                    """
                    UPDATE nba_players
                    SET injury = $1::jsonb
                    WHERE player_id = $2
                    """,
                    json.dumps([inj]),
                    pid,
                )

            # Clear injuries for all other players
            if players_with_injury:
                await conn.execute(
                    """
                    UPDATE nba_players
                    SET injury = NULL
                    WHERE player_id <> ALL($1::bigint[])
                      AND injury IS NOT NULL
                    """,
                    players_with_injury,
                )

        # Summary
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM nba_players WHERE injury IS NOT NULL AND injury != 'null'::jsonb"
            )
            logger.info(f"Done. {len(latest)} injuries upserted, {count} players now have active injuries.")

    logger.info("NBA injury update complete")
