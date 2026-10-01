"""Bounded play-stat audit; derived labels remain separate from box-score truth."""
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from cfb.data import Client,connect,ROOT


def reconcile_receivers(events, observed, expected_receptions, expected_yards):
    if len(events)>=2000: return False,{},'response_cap'
    receptions=defaultdict(lambda:[0,0]); targets=defaultdict(int); seen=set()
    for r in events:
        key=(r['playId'],str(r['athleteId']),r['statType'])
        if key in seen: return False,{},'duplicate_event'
        seen.add(key)
        pid=str(r['athleteId'])
        if r['statType']=='Reception':
            value=r['stat']
            if value is None or int(value)!=value: return False,{},'invalid_yardage'
            receptions[pid][0]+=1; receptions[pid][1]+=int(value)
        elif r['statType']=='Target':
            if r['stat']!=1: return False,{},'invalid_target'
            targets[pid]+=1
    if expected_receptions is None or expected_yards is None: return False,{},'missing_team_totals'
    if sum(v[0] for v in receptions.values())!=expected_receptions or sum(v[1] for v in receptions.values())!=expected_yards:
        return False,{},'team_totals_mismatch'
    if any(tuple(receptions.get(p,(0,0)))!=tuple(v) for p,v in observed.items()):
        return False,{},'player_totals_mismatch'
    if any(p not in observed for p in receptions): return False,{},'unmatched_receiver'
    zeros={p:n for p,n in targets.items() if p not in receptions and p not in observed}
    return True,zeros,'reconciled'


def run(limit=60):
    if not 1<=limit<=200: raise ValueError('Pilot limit must be 1..200 games')
    with connect() as conn:
        # Deterministic development-only sample; no 2026 selection or result peeking.
        ids=[r[0] for r in conn.execute('''SELECT DISTINCT p.game_id FROM cfb_model_v1.player_predictions p
        JOIN cfb_model_v1.games g USING(game_id) WHERE p.category='receiving' AND p.actual_yards IS NULL
        AND g.season BETWEEN 2023 AND 2025 ORDER BY p.game_id''').fetchall()]
    import random
    random.Random(42).shuffle(ids); ids=ids[:limit]
    def fetch(gid): return gid,Client(max_calls=3).get('/plays/stats',{'gameId':gid})
    counts={'games':len(ids),'passed_teams':0,'failed_teams':0,'derived_zeros':0,'failures':{}}
    with ThreadPoolExecutor(max_workers=3) as pool:
        for gid,events in pool.map(fetch,ids):
            with connect() as conn:
                g=conn.execute('SELECT home_id,away_id,home_team,away_team FROM cfb_model_v1.games WHERE game_id=%s',(gid,)).fetchone()
                for tid,name in [(g[0],g[2]),(g[1],g[3])]:
                    t=conn.execute('SELECT payload,pass_yards FROM cfb_model_v1.team_game_stats WHERE game_id=%s AND team_id=%s',(gid,tid)).fetchone()
                    raw={s['category']:s['stat'] for s in t[0]['stats']} if t else {}
                    try: completions=int(raw['completionAttempts'].split('-')[0])
                    except (KeyError,ValueError): completions=None
                    observed={str(r[0]):(r[1],r[2]) for r in conn.execute("SELECT player_id,volume,yards FROM cfb_model_v1.player_offense WHERE game_id=%s AND team_id=%s AND category='receiving'",(gid,tid)).fetchall()}
                    valid_source=all(int(r['gameId'])==gid and r['team'] in (g[2],g[3]) for r in events)
                    if len(events)>=2000 or not valid_source:
                        passed,zeros,reason=False,{},'cap_or_identity_failure'
                    else:
                        passed,zeros,reason=reconcile_receivers([r for r in events if r['team']==name],observed,completions,t[1] if t else None)
                    evidence={'events':len(events),'reason':reason,'observed_receivers':len(observed),'derived_zeros':zeros,
                              'scope':'Pilot derived labels only; original box labels unchanged'}
                    conn.execute('''INSERT INTO cfb_model_v1.play_label_audits(game_id,team_id,passed,evidence) VALUES (%s,%s,%s,%s)
                    ON CONFLICT(game_id,team_id) DO UPDATE SET passed=excluded.passed,evidence=excluded.evidence,retrieved_at=now()''',(gid,tid,passed,json.dumps(evidence)))
                    # Replace only this task's derived audit labels, including revocation after a failed re-audit.
                    conn.execute('DELETE FROM cfb_model_v1.play_derived_zeros WHERE game_id=%s AND team_id=%s',(gid,tid))
                    with conn.pipeline():
                        for pid,n in zeros.items():
                            conn.execute('INSERT INTO cfb_model_v1.play_derived_zeros(game_id,team_id,player_id,targets,category) VALUES (%s,%s,%s,%s,%s)',(gid,tid,pid,n,'receiving'))
                    counts['passed_teams' if passed else 'failed_teams']+=1
                    counts['derived_zeros']+=len(zeros)
                    if not passed: counts['failures'][reason]=counts['failures'].get(reason,0)+1
            print(f'Play audit game {gid}: processed',flush=True)
    out=ROOT/'outputs'/'play_label_pilot.json'; out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(counts,indent=2)); print(json.dumps(counts),flush=True)
