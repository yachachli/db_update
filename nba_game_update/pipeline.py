"""
Pipeline: Fetch yesterday's + today's NBA game results from Tank01
and upsert into nba_game_results in Neon.
"""

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import get_engine, query_df
from .tank01_client import Tank01Client
from .game_parser import parse_game

UPSERT_SQL = text("""
    INSERT INTO nba_game_results
        (game_id, game_date, home_team, away_team,
         home_score, away_score, season, status)
    VALUES
        (:game_id, :game_date, :home_team, :away_team,
         :home_score, :away_score, :season, :status)
    ON CONFLICT (game_id) DO UPDATE SET
        home_score = EXCLUDED.home_score,
        away_score = EXCLUDED.away_score,
        status     = EXCLUDED.status,
        updated_at = NOW()
""")


def run_game_update(engine):
    api_key = os.getenv("TANK01_API_KEY")
    if not api_key:
        raise RuntimeError("TANK01_API_KEY not set")

    pst = timezone(timedelta(hours=-8))
    now_pst = datetime.now(pst)
    yesterday = (now_pst - timedelta(days=1)).strftime("%Y%m%d")
    today = now_pst.strftime("%Y%m%d")

    print(f"[{now_pst.isoformat()}] NBA Game Update starting...")
    print(f"  Fetching dates: {yesterday}, {today}")

    client = Tank01Client(api_key)
    total = 0

    for date_str in [yesterday, today]:
        raw_games = client.get_scores_for_date(date_str)
        records = [parse_game(g, fallback_date=date_str) for g in (raw_games or [])]
        records = [r for r in records if r is not None]

        if records:
            with engine.begin() as conn:
                for rec in records:
                    conn.execute(UPSERT_SQL, rec)
            total += len(records)
            print(f"  {date_str}: {len(records)} games upserted")
        else:
            print(f"  {date_str}: 0 games found")

    summary = query_df("""
        SELECT COUNT(*) AS total_games, MAX(game_date) AS latest
        FROM nba_game_results
    """)
    print(f"  Neon total: {summary['total_games'].iloc[0]} games, "
          f"latest: {summary['latest'].iloc[0]}")
    print(f"  Done. {total} games updated.")
