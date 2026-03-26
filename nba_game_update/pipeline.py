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


def fetch_and_upsert_games(engine, client: Tank01Client, date_str: str) -> int:
    """Fetch games for a specific date and upsert them. Returns count of games upserted."""
    raw_games = client.get_scores_for_date(date_str)
    
    if not raw_games:
        print(f"  {date_str}: 0 games found (API returned empty)")
        return 0
    
    records = [parse_game(g, fallback_date=date_str) for g in raw_games]
    records = [r for r in records if r is not None]
    
    if not records:
        print(f"  {date_str}: 0 games parsed (raw games: {len(raw_games)}, parsed: 0)")
        return 0

    try:
        with engine.begin() as conn:
            for rec in records:
                conn.execute(UPSERT_SQL, rec)
        print(f"  {date_str}: {len(records)} games upserted into nba_game_results")
        return len(records)
    except Exception as e:
        print(f"  ERROR upserting {date_str}: {e}")
        import traceback
        traceback.print_exc()
        return 0


def detect_missing_dates(engine, start_date: datetime, end_date: datetime) -> list[str]:
    """Detect dates between start_date and end_date that have no games in the database."""
    # Get all dates that should have games (NBA season typically Oct-Apr)
    # Query the database for existing game dates
    existing_dates_df = query_df("""
        SELECT DISTINCT game_date::date as date
        FROM nba_game_results
        WHERE game_date >= :start_date AND game_date <= :end_date
        ORDER BY date
    """, {"start_date": start_date.date(), "end_date": end_date.date()})
    
    existing_dates = set(existing_dates_df['date'].dt.strftime('%Y%m%d').tolist()) if not existing_dates_df.empty else set()
    
    # Generate all dates in range
    all_dates = set()
    current = start_date
    while current <= end_date:
        all_dates.add(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    # Find missing dates
    missing = sorted(all_dates - existing_dates)
    return missing


def run_game_update(engine, backfill_days: int = 0):
    """
    Run game update pipeline.
    
    Args:
        engine: SQLAlchemy engine
        backfill_days: If > 0, check for and backfill missing dates going back this many days
    """
    # RAPIDAPI_KEY = db_update repo convention; TANK01_API_KEY = nba_predictor
    api_key = os.getenv("RAPIDAPI_KEY") or os.getenv("TANK01_API_KEY")
    if not api_key:
        raise RuntimeError("Set RAPIDAPI_KEY or TANK01_API_KEY")

    pst = timezone(timedelta(hours=-8))
    now_pst = datetime.now(pst)
    yesterday = (now_pst - timedelta(days=1)).strftime("%Y%m%d")
    today = now_pst.strftime("%Y%m%d")

    print(f"[{now_pst.isoformat()}] NBA Game Update starting...")
    print(f"  Fetching dates: {yesterday}, {today}")

    client = Tank01Client(api_key)
    total = 0

    # Always fetch yesterday and today
    for date_str in [yesterday, today]:
        total += fetch_and_upsert_games(engine, client, date_str)

    # Optional backfill: check for missing dates
    if backfill_days > 0:
        print(f"\n  Checking for missing dates in last {backfill_days} days...")
        start_date = now_pst - timedelta(days=backfill_days)
        end_date = now_pst - timedelta(days=1)  # Don't backfill today, already fetched
        
        missing_dates = detect_missing_dates(engine, start_date, end_date)
        
        if missing_dates:
            print(f"  Found {len(missing_dates)} missing dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
            for date_str in missing_dates:
                total += fetch_and_upsert_games(engine, client, date_str)
        else:
            print(f"  No missing dates found in last {backfill_days} days")

    summary = query_df("""
        SELECT COUNT(*) AS total_games, MAX(game_date) AS latest
        FROM nba_game_results
    """)
    print(f"\n  Neon total: {summary['total_games'].iloc[0]} games, "
          f"latest: {summary['latest'].iloc[0]}")
    print(f"  Done. {total} games updated.")
