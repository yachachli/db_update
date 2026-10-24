"""
Pipeline: Read game results from Neon, calculate AdjEM team ratings,
and upsert into nba_team_ratings.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

from .db import get_engine, query_df

def current_season():
    today = datetime.now(ZoneInfo("America/New_York"))
    start = today.year if today.month >= 10 else today.year-1
    return os.getenv("NBA_SEASON", f"{start}-{(start+1)%100:02d}")

UPSERT_SQL = text("""
    INSERT INTO nba_team_ratings
        (team, season, adj_em, adj_off, adj_def, games, last_game_date, calculated_at)
    VALUES
        (:team, :season, :adj_em, :adj_off, :adj_def, :games, :last_game_date, NOW())
    ON CONFLICT (team, season) DO UPDATE SET
        adj_em         = EXCLUDED.adj_em,
        adj_off        = EXCLUDED.adj_off,
        adj_def        = EXCLUDED.adj_def,
        games          = EXCLUDED.games,
        last_game_date = EXCLUDED.last_game_date,
        calculated_at  = NOW()
""")


def run_ratings_update(engine):
    import json
    from pathlib import Path
    from .efficiency import load_efficiencies
    season = current_season()
    start = int(season[:4])
    previous = f"{start-1}-{start%100:02d}"
    cutoff = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
    manifest = json.loads(Path(__file__).with_name('regular_season_manifest.json').read_text())
    allowed = set(manifest['seasons'][previous]['game_ids'])
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text("SELECT * FROM nba_game_results WHERE season IN (:season,:previous) AND status='completed' AND game_date < :cutoff"),
            dict(season=season,previous=previous,cutoff=cutoff)).mappings()]
    prior = [r for r in rows if r['season']==previous and r['game_id'] in allowed]
    if {r['game_id'] for r in prior} != allowed:
        raise RuntimeError('Prior regular-season results incomplete')
    # The current-season game schedule excludes preseason from the rating input.
    import requests
    key = os.getenv('RAPIDAPI_KEY') or os.environ['TANK01_API_KEY']
    response = requests.get('https://tank01-fantasy-stats.p.rapidapi.com/getNBATeams',
        params={'schedules':'true'}, headers={'X-RapidAPI-Key':key,
        'X-RapidAPI-Host':'tank01-fantasy-stats.p.rapidapi.com'},timeout=60)
    response.raise_for_status()
    teams = response.json().get('body')
    if not isinstance(teams,list) or len(teams)!=30:
        raise RuntimeError('Incomplete NBA schedule')
    current_ids=set()
    for team in teams:
        schedule=team.get('teamSchedule')
        if not isinstance(schedule,dict) or not schedule:
            raise RuntimeError('Missing NBA team schedule')
        current_ids.update(g['gameID'] for g in schedule.values() if g.get('seasonType')=='Regular Season')
    current=[r for r in rows if r['season']==season and r['game_id'] in current_ids]
    ratings=load_efficiencies(prior,current,cutoff,engine.raw_connection)
    if len(ratings)!=30 or any(r.get('status')!='measured' for r in ratings.values()):
        raise RuntimeError('Insufficient box-score coverage for team efficiencies')
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE nba_team_ratings ADD COLUMN IF NOT EXISTS rating_method TEXT, ADD COLUMN IF NOT EXISTS prior_games INTEGER, ADD COLUMN IF NOT EXISTS current_weight DOUBLE PRECISION"))
        for team,r in ratings.items():
            games=[g for g in current if team in (g['home_team'],g['away_team'])]
            if r['current_games'] != len(games):
                raise RuntimeError(f'Incomplete current box scores for {team}')
            conn.execute(UPSERT_SQL,dict(team=team,season=season,adj_em=r['adj_off']-r['adj_def'],
                adj_off=r['adj_off'],adj_def=r['adj_def'],games=r['current_games'],
                last_game_date=max((g['game_date'] for g in games),default=None)))
            conn.execute(text("UPDATE nba_team_ratings SET rating_method=:method,prior_games=:prior,current_weight=:weight WHERE team=:team AND season=:season"),
                dict(method=r['method'],prior=r['prior_games'],weight=r['current_weight'],team=team,season=season))
    print(f"Stored {len(ratings)} measured season-blended efficiencies for {season}; defense range "
          f"{min(r['adj_def'] for r in ratings.values()):.2f}–{max(r['adj_def'] for r in ratings.values()):.2f}")
