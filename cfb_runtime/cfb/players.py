"""Audited offensive player history; absent category rows are never zero-filled."""
import json
from cfb.data import Client, ROOT, connect

VOLUME = {'passing':'C/ATT','rushing':'CAR','receiving':'REC'}


def parse(category, values):
    yards=values.get('YDS'); volume=values.get(VOLUME[category])
    if yards in (None,'','--') or volume in (None,'','--'):
        return None
    if category=='passing':
        completed,volume=map(int,volume.split('/'))
        if not 0<=completed<=volume: raise ValueError('Invalid player completions')
    else: volume=int(volume)
    if volume<0: raise ValueError('Negative player volume')
    return volume,int(yards)


def ingest(years, recent_partitions=None, refresh=False):
    client=Client(max_calls=300)
    reports=[]
    for year in years:
        with connect() as conn:
            rows=conn.execute('SELECT game_id,week,season_type,home_id,away_id,home_team,away_team,kickoff FROM cfb_model_v1.games WHERE completed AND season=%s',(year,)).fetchall()
        from cfb.partitions import recent
        rows=recent(rows,recent_partitions)
        games={r[0]:r for r in rows}; seen=set(); count=0; missing=[]
        for week,kind in sorted({(r[1],r[2]) for r in rows}):
            payload=client.get('/games/players',{'year':year,'week':week,'seasonType':kind,'classification':'fbs'},cache=not refresh)
            with connect() as conn,conn.pipeline():
                for game in payload:
                    gid=game['id']
                    if gid not in games: raise ValueError('Player game missing from completed schedule')
                    g=games[gid]; teams={g[5]:g[3],g[6]:g[4]}
                    if {t['team'] for t in game['teams']}!=set(teams): raise ValueError('Player team mapping mismatch')
                    seen.add(gid)
                    for team in game['teams']:
                        for category in team['categories']:
                            name=category['name']
                            if name not in VOLUME: continue
                            players={}
                            for stat in category['types']:
                                if stat['name'] not in ('YDS',VOLUME[name]): continue
                                for athlete in stat['athletes']:
                                    pid=str(athlete['id'])
                                    p=players.setdefault(pid,{'name':athlete['name'],'values':{}})
                                    if stat['name'] in p['values']: raise ValueError('Duplicate player metric')
                                    p['values'][stat['name']]=athlete['stat']
                            for pid,p in players.items():
                                values=parse(name,p['values'])
                                if values is None:
                                    missing.append({'game_id':gid,'player_id':pid,'category':name})
                                    continue
                                conn.execute('''INSERT INTO cfb_model_v1.player_offense VALUES (%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT(game_id,player_id,category) DO UPDATE SET team_id=excluded.team_id,
                                player_name=excluded.player_name,volume=excluded.volume,yards=excluded.yards''',
                                (gid,pid,teams[team['team']],p['name'],name,*values))
                                count+=1
            print(f'{year} {kind} week {week}: player history stored',flush=True)
        report={'year':year,'expected_games':len(games),'returned_games':len(seen),'missing_game_ids':sorted(set(games)-seen),
                'offensive_rows':count,'incomplete_player_rows':missing}
        reports.append(report)
        print(json.dumps(report),flush=True)
    out=ROOT/'outputs'; out.mkdir(exist_ok=True)
    (out/'player_ingestion_audit.json').write_text(json.dumps(reports,indent=2))
