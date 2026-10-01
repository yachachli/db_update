"""Normalize box scores without turning missing values into zero."""
import json
from cfb.data import Client, connect


def normalize(team):
    stats = {s['category']: s['stat'] for s in team['stats']}
    def integer(key):
        value = stats.get(key)
        if value in (None, '', '--'):
            return None
        return int(value)
    pair = stats.get('completionAttempts')
    attempts = None
    if pair not in (None, '', '--'):
        completions, attempts = map(int, pair.split('-'))
        if not 0 <= completions <= attempts:
            raise ValueError('Invalid completions/attempts')
    rush_attempts = integer('rushingAttempts')
    if rush_attempts is not None and rush_attempts < 0:
        raise ValueError('Negative rushing attempts')
    return integer('netPassingYards'), attempts, integer('rushingYards'), rush_attempts


def ingest(years, recent_partitions=None, refresh=False):
    client = Client(max_calls=300)
    for year in years:
        with connect() as conn:
            games = conn.execute('SELECT game_id,week,season_type,home_id,away_id,kickoff FROM cfb_model_v1.games WHERE season=%s AND completed', (year,)).fetchall()
        from cfb.partitions import recent
        games = recent(games, recent_partitions)
        known = {g[0]: g for g in games}
        inserted = 0
        for week, kind in sorted({(g[1], g[2]) for g in games}):
            rows = client.get('/games/teams', {'year':year, 'week':week, 'seasonType':kind, 'classification':'fbs'}, cache=not refresh)
            with connect() as conn, conn.pipeline():
                for game in rows:
                    if game['id'] not in known:
                        raise ValueError('Box-score game missing from completed schedule; refresh games first')
                    expected = set(known[game['id']][3:5])
                    if {t['teamId'] for t in game['teams']} != expected:
                        raise ValueError('Box-score participant mismatch')
                    for team in game['teams']:
                        opponent = next(iter(expected - {team['teamId']}))
                        conn.execute('''INSERT INTO cfb_model_v1.team_game_stats
                        (game_id,team_id,opponent_id,pass_yards,pass_attempts,rush_yards,rush_attempts,payload)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(game_id,team_id) DO UPDATE SET
                        pass_yards=excluded.pass_yards,pass_attempts=excluded.pass_attempts,
                        rush_yards=excluded.rush_yards,rush_attempts=excluded.rush_attempts,
                        payload=excluded.payload,retrieved_at=now()''',
                        (game['id'],team['teamId'],opponent,*normalize(team),json.dumps(team)))
                        inserted += 1
        print(f'{year}: {inserted} team box scores stored', flush=True)
