"""Ingest complete game boxes, including players absent from the roster table."""
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from sqlalchemy import text
from .db import get_engine
from .game_parser import standardize_team

FIELDS = {'minutes_played':'mins','points':'pts','rebounds':'reb','assists':'ast',
 'steals':'stl','blocks':'blk','turnovers':'TOV','offensive_rebounds':'OffReb',
 'defensive_rebounds':'DefReb','free_throw_percentage':'ftp','plus_minus':'plusMinus',
 'technical_fouls':'tech','field_goal_attempts':'fga','three_point_fg_percentage':'tptfgp',
 'field_goals_made':'fgm','field_goal_percentage':'fgp','three_point_fg_made':'tptfgm',
 'free_throw_attempts':'fta','three_point_fg_attempts':'tptfga','personal_fouls':'PF',
 'free_throws_made':'ftm','fantasy_points':'fantasyPoints'}

def validate_box(game, payload):
    body = payload.get('body', {})
    players = body.get('playerStats', {})
    if not players or str(body.get('gameStatus', '')).lower() not in ('completed','final'):
        raise RuntimeError(f"Incomplete box score: {game['game_id']}")
    rows=[]
    for pid,p in players.items():
        if p.get('mins') in (None, '', '--', 'DNP') or float(p['mins']) <= 0:
            continue
        if any(p.get(k) is None for k in ('pts','fga','fta','OffReb','TOV','teamID','teamAbv','longName')):
            raise RuntimeError('Missing required box-score field')
        team=standardize_team(p['teamAbv'])
        if team not in (game['home_team'],game['away_team']):
            raise RuntimeError('Box-score team mismatch')
        row={k:float(p.get(v) or 0) for k,v in FIELDS.items()}
        row.update(player_id=int(pid),game_id=game['game_id'],team_id=int(p['teamID']),
            team_abv=p['teamAbv'],game_date=game['game_date'],name=p['longName'],
            home_away='Home' if team==game['home_team'] else 'Away',
            opponent=game['away_team'] if team==game['home_team'] else game['home_team'])
        rows.append(row)
    for side in ('home','away'):
        team_rows=[r for r in rows if standardize_team(r['team_abv'])==game[side+'_team']]
        if abs(sum(r['points'] for r in team_rows)-game[side+'_score'])>1 or sum(r['minutes_played'] for r in team_rows)<225:
            raise RuntimeError(f"Box score does not reconcile: {game['game_id']} {side}")
    return rows

def run_boxscores(engine, days=7, skip_complete=False):
    start=datetime.now(ZoneInfo('America/New_York')).date()-timedelta(days=days)
    with engine.connect() as conn:
        games=[dict(r) for r in conn.execute(text("SELECT * FROM nba_game_results WHERE game_date>=:start AND status='completed' ORDER BY game_date"),{'start':start}).mappings()]
    key=os.environ.get('RAPIDAPI_KEY') or os.environ['TANK01_API_KEY']
    cols=['player_id','game_id','team_id',*FIELDS,'home_away','opponent','game_date','team_abv']
    sql=text('INSERT INTO nba_player_game_stats ('+','.join(cols)+') VALUES ('+','.join(':'+c for c in cols)+') ON CONFLICT (player_id,game_id) DO UPDATE SET '+','.join(c+'=EXCLUDED.'+c for c in cols if c not in ('player_id','game_id')))
    for game in games:
        if skip_complete:
            with engine.connect() as conn:
                stored={r['home_away']: r for r in conn.execute(text('SELECT home_away,SUM(points) AS points,SUM(minutes_played) AS minutes,COUNT(*) FILTER (WHERE field_goal_attempts IS NULL OR free_throw_attempts IS NULL OR offensive_rebounds IS NULL OR turnovers IS NULL) AS missing FROM nba_player_game_stats WHERE game_id=:game_id GROUP BY home_away'),game).mappings()}
            if all(stored.get(side.title(),{}).get('points')==game[side+'_score'] and float(stored[side.title()]['minutes'] or 0)>=225 and stored[side.title()]['missing']==0 for side in ('home','away')):
                print(f"Already complete: {game['game_id']}",flush=True)
                continue
        response=requests.get('https://tank01-fantasy-stats.p.rapidapi.com/getNBABoxScore',params={'gameID':game['game_id']},headers={'X-RapidAPI-Key':key,'X-RapidAPI-Host':'tank01-fantasy-stats.p.rapidapi.com'},timeout=60)
        response.raise_for_status()
        rows=validate_box(game,response.json())
        with engine.begin() as conn:
            for row in rows:
                known = conn.execute(text('SELECT player_id FROM nba_players WHERE player_id=:player_id'),row).scalar()
                if known is None:
                    # Tank01 occasionally changes an external ID for an existing player.
                    # Preserve established FK identity for an exact unique name/team match.
                    known = conn.execute(text('SELECT player_id FROM nba_players WHERE name=:name AND team_id=:team_id'),row).scalar()
                    if known is not None:
                        print(f"Resolved player ID alias {row['player_id']} -> {known}: {row['name']}",flush=True)
                        row['player_id'] = known
                    else:
                        conn.execute(text('INSERT INTO nba_players (player_id,name,team_id) VALUES (:player_id,:name,:team_id) ON CONFLICT (player_id) DO NOTHING'),row)
                conn.execute(sql,row)
            stored = {r['home_away']: r for r in conn.execute(text('SELECT home_away,SUM(points) AS points,SUM(minutes_played) AS minutes FROM nba_player_game_stats WHERE game_id=:game_id GROUP BY home_away'),game).mappings()}
            for side in ('home','away'):
                actual=stored.get(side.title(),{})
                if abs(float(actual.get('points',-999))-game[side+'_score'])>1 or float(actual.get('minutes',0))<225:
                    raise RuntimeError(f"Stored box scores do not reconcile: {game['game_id']}")
        print(f"Verified and stored {game['game_id']}: {len(rows)} player boxes",flush=True)
        time.sleep(.6)

if __name__=='__main__':
    run_boxscores(get_engine())
