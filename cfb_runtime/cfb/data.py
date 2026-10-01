"""Bounded HTTP ingestion and isolated PostgreSQL storage."""
import hashlib
import json
import os
from pathlib import Path
import time
from datetime import datetime, timezone

import psycopg
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
SCHEMA = 'cfb_model_v1'


def connect():
    url = os.environ.get('DATABASE_URL', '')
    if not url.startswith(('postgres://', 'postgresql://')):
        raise ValueError('DATABASE_URL must point to PostgreSQL/Neon')
    conn = psycopg.connect(url, connect_timeout=10)
    try:
        # Neon transaction poolers reject statement_timeout in startup options.
        # All callers use one connection-context transaction: keep the setting
        # local to that transaction, with no session state leaking to the pool.
        conn.execute("SET LOCAL statement_timeout = '30s'")
    except Exception:
        conn.close()
        raise
    return conn


def migrate():
    with connect() as conn:
        for path in sorted((ROOT / 'migrations').glob('*.sql')):
            conn.execute(path.read_text())


class Client:
    def __init__(self, provider='cfbd', max_calls=100):
        self.provider, self.max_calls, self.calls = provider, max_calls, 0
        self.base = os.environ.get('CFBD_BASE_URL', 'https://api.collegefootballdata.com') if provider == 'cfbd' else os.environ.get('ODDS_API_BASE_URL', 'https://api.the-odds-api.com/v4')
        self.key = os.environ['CFBD_API_KEY' if provider == 'cfbd' else 'ODDS_API_KEY']

    def get(self, endpoint, params=None, cache=True):
        params = dict(params or {})
        digest = hashlib.sha256(json.dumps([self.provider, endpoint, params], sort_keys=True).encode()).hexdigest()
        path = ROOT / 'data/raw' / (digest + '.json')
        if cache and path.exists():
            record = json.loads(path.read_text())
            # Prior seasons can still include January postseason games.
            historical = int(params.get('year', datetime.now(timezone.utc).year)) < datetime.now(timezone.utc).year - 1
            if historical or time.time() - record['retrieved_at'] < 3600:
                return record['payload']
        headers = {'Authorization': f'Bearer {self.key}'} if self.provider == 'cfbd' else {}
        query = params if self.provider == 'cfbd' else {**params, 'apiKey': self.key}
        for attempt in range(3):
            if self.calls >= self.max_calls:
                raise RuntimeError('Per-run API call budget exhausted')
            self.calls += 1
            try:
                response = requests.get(self.base + endpoint, params=query, headers=headers, timeout=(5, 20))
            except requests.RequestException:
                if attempt == 2:
                    raise RuntimeError(f'{self.provider} network failure (credentials redacted)') from None
                time.sleep(1 + attempt)
                continue
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < 2:
                    time.sleep(1 + attempt)
                    continue
            if response.status_code != 200:
                raise RuntimeError(f'{self.provider} {endpoint}: HTTP {response.status_code}')
            payload = response.json()
            record = {'provider': self.provider, 'endpoint': endpoint, 'params': params, 'retrieved_at': time.time(), 'payload': payload}
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(record))
            with connect() as conn:
                conn.execute('INSERT INTO cfb_model_v1.ingestion_runs(provider,endpoint,parameters,row_count,quota_remaining) VALUES (%s,%s,%s,%s,%s)', (self.provider, endpoint, json.dumps(params), len(payload) if isinstance(payload,list) else 1, response.headers.get('x-requests-remaining')))
            return payload
        raise RuntimeError('Request retries exhausted')


def ingest_games(years, refresh=False):
    client = Client()
    for year in years:
        rows = client.get('/games', {'year': year, 'seasonType': 'both', 'classification': 'fbs'}, cache=not refresh)
        with connect() as conn, conn.pipeline():
            for r in rows:
                if r['season'] != year:
                    raise ValueError('API returned wrong season')
                conn.execute('''INSERT INTO cfb_model_v1.games
                    (game_id,season,week,season_type,kickoff,home_id,away_id,home_team,away_team,home_classification,away_classification,neutral_site,completed,home_points,away_points,payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT(game_id) DO UPDATE SET completed=excluded.completed,home_points=excluded.home_points,away_points=excluded.away_points,payload=excluded.payload,kickoff=excluded.kickoff''',
                    (r['id'],year,r['week'],r['seasonType'],r['startDate'],r['homeId'],r['awayId'],r['homeTeam'],r['awayTeam'],r.get('homeClassification'),r.get('awayClassification'),r.get('neutralSite',False),r['completed'],r.get('homePoints'),r.get('awayPoints'),json.dumps(r)))
        print(f'{year}: {len(rows)} games stored', flush=True)


def ingest_players(year, week):
    rows = Client().get('/games/players', {'year':year, 'week':week, 'seasonType':'regular', 'classification':'fbs'})
    with connect() as conn, conn.pipeline():
        for game in rows:
            for team in game['teams']:
                for category in team['categories']:
                    for stat in category['types']:
                        for athlete in stat['athletes']:
                            conn.execute('''INSERT INTO cfb_model_v1.player_game_stats(game_id,player_id,team,player_name,category,stat_type,stat_value)
                            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(game_id,player_id,category,stat_type) DO UPDATE SET stat_value=excluded.stat_value''',
                            (game['id'],str(athlete['id']),team['team'],athlete['name'],category['name'],stat['name'],athlete['stat']))
    print(f'Player box scores: {len(rows)} games stored')


def snapshot_odds():
    client = Client('odds', max_calls=5)
    events = client.get('/sports/americanfootball_ncaaf/odds', {'regions':'us','markets':'h2h,spreads,totals','oddsFormat':'american'}, cache=False)
    with connect() as conn, conn.pipeline():
        for event in events:
            conn.execute('INSERT INTO cfb_model_v1.odds_snapshots(event_id,payload) VALUES (%s,%s)', (event['id'],json.dumps(event)))
    print(f'Odds snapshots: {len(events)} events stored')
