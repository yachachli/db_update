"""Current Tank01 membership, separate from immutable player/stat identities."""
from collections import defaultdict
import re
import unicodedata

ALIASES = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'PHO': 'PHX', 'SA': 'SAS'}
TEAMS = set('ATL BOS BKN CHA CHI CLE DAL DEN DET GSW HOU IND LAC LAL MEM MIA MIL MIN NOP NYK OKC ORL PHI PHX POR SAC SAS TOR UTA WAS'.split())
DDL = """CREATE TABLE IF NOT EXISTS nba_current_rosters (
 player_id bigint PRIMARY KEY REFERENCES nba_players(player_id),
 provider_player_id bigint NOT NULL UNIQUE,
 team_id integer NOT NULL REFERENCES nba_teams(id),
 synced_at timestamptz NOT NULL DEFAULT now()
)"""

def identity(name):
    value = ''.join(c for c in unicodedata.normalize('NFKD', name) if not unicodedata.combining(c))
    value = re.sub(r'\b(jr|sr|ii|iii|iv)\b', '', value.lower().replace('.', ''))
    return re.sub('[^a-z0-9]', '', value)


def roster_plan(payload, players, teams):
    """Validate entire snapshot and resolve unique IDs before any writes."""
    if payload.get('statusCode') != 200 or not isinstance(payload.get('body'), list):
        raise ValueError('Tank01 roster response failed')
    team_ids = {ALIASES.get(t['team_abv'], t['team_abv']): int(t['id']) for t in teams}
    by_id = {int(p['player_id']): p for p in players}
    by_name = defaultdict(list)
    for p in players:
        by_name[identity(p['name'])].append(p)
    seen_teams, seen_provider, seen_canonical = set(), set(), set()
    plan = []
    for team in payload['body']:
        abv = ALIASES.get(team['teamAbv'], team['teamAbv'])
        roster = team.get('Roster')
        if abv not in TEAMS or abv in seen_teams or not isinstance(roster, dict) or not 10 <= len(roster) <= 25:
            raise ValueError('Incomplete or duplicate team roster')
        seen_teams.add(abv)
        tid = team_ids[abv]
        if int(team['teamID']) != tid:
            raise ValueError('Provider/database team identity conflict')
        for pid, player in roster.items():
            pid = int(pid)
            if pid != int(player['playerID']) or pid in seen_provider:
                raise ValueError('Duplicate or inconsistent provider player ID')
            if int(player['teamID']) != tid or ALIASES.get(player['team'], player['team']) != abv:
                raise ValueError('Player and roster team conflict')
            seen_provider.add(pid)
            names = {identity(player[k]) for k in ('longName','nbaComName','espnName','bRefName') if player.get(k)}
            existing = by_id.get(pid)
            if existing and identity(existing['name']) not in names:
                raise ValueError('Provider ID/name identity conflict')
            if existing is None:
                candidates = {int(p['player_id']): p for name in names for p in by_name.get(name, [])}
                if len(candidates) > 1:
                    raise ValueError('Ambiguous player alias: ' + player['longName'])
                existing = next(iter(candidates.values()), None)
            canonical = int(existing['player_id']) if existing else pid
            if canonical in seen_canonical:
                raise ValueError('Two roster entries resolve to one player')
            seen_canonical.add(canonical)
            plan.append(dict(player_id=canonical, provider_player_id=pid, name=player['longName'],
                             position=player.get('pos') or '', team_id=tid, team_abv=abv,
                             player_pic=player.get('nbaComHeadshot') or player.get('espnHeadshot') or '',
                             existing=existing is not None))
    if seen_teams != TEAMS:
        raise ValueError('Roster snapshot must contain all 30 teams')
    return plan


async def sync_rosters(pool, payload):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(20261027, 1)")
            players = await conn.fetch('SELECT player_id,name,team_id FROM nba_players')
            teams = await conn.fetch('SELECT id,team_abv FROM nba_teams')
            plan = roster_plan(payload, players, teams)
            await conn.execute(DDL)
            for p in plan:
                await conn.execute('''INSERT INTO nba_players (player_id,name,position,team_id,player_pic)
                    VALUES ($1,$2,$3,$4,$5) ON CONFLICT (player_id) DO UPDATE SET
                    team_id=EXCLUDED.team_id,
                    position=COALESCE(NULLIF(EXCLUDED.position,''),nba_players.position),
                    player_pic=COALESCE(NULLIF(EXCLUDED.player_pic,''),nba_players.player_pic)''',
                    p['player_id'],p['name'],p['position'],p['team_id'],p['player_pic'])
            # Refresh atomically; readers see either the previous or complete new snapshot.
            await conn.execute('DELETE FROM nba_current_rosters')
            await conn.executemany('''INSERT INTO nba_current_rosters
                (player_id,provider_player_id,team_id) VALUES ($1,$2,$3)''',
                [(p['player_id'],p['provider_player_id'],p['team_id']) for p in plan])
    return plan
