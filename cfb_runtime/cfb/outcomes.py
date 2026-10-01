"""Postgame evidence audit. Never use these fields as pregame features."""
import json
from collections import defaultdict
import pandas as pd
from cfb.data import ROOT,connect

ACTIVITY={'passing':{'C/ATT'},'rushing':{'CAR'},'receiving':{'REC'},
          'defensive':{'TOT','SACKS','TFL','PD','QB HUR'},'interceptions':{'INT'},
          'fumbles':{'FUM','REC'},'kickReturns':{'NO'},'puntReturns':{'NO'},
          'punting':{'NO'},'kicking':{'FG','XP'}}


def positive_activity(category, stat, value):
    if stat not in ACTIVITY.get(category,set()): return False
    try:
        # An attempted pass/kick is evidence even if no completion/make occurred.
        number=float(str(value).split('/')[-1])
        return number>0
    except (TypeError,ValueError): return False


def evidence_from_cache(games,wanted):
    """Use the existing cached historical snapshots; no bulk API repull."""
    latest={}; evidence={}; play_evidence=defaultdict(list)
    for path in sorted((ROOT/'data/raw').glob('*.json')):
        record=json.loads(path.read_text())
        endpoint=record.get('endpoint')
        if endpoint=='/plays/stats':
            payload=record['payload']
            # A cap hit cannot establish completeness, but positive events remain evidence.
            for r in payload:
                gid=int(r['gameId'])
                if gid not in games: continue
                names=games[gid]; tid=names.get(r['team'])
                key=(gid,str(r['athleteId']),tid)
                if key in wanted:
                    play_evidence[key].append({'type':r['statType'],'play_id':r['playId'],'cache':path.name,
                                               'retrieved_at':record['retrieved_at'],'cap_hit':len(payload)>=2000})
            continue
        if endpoint!='/games/players': continue
        for game in record['payload']:
            gid=game['id']
            if gid not in games or record['retrieved_at']<=latest.get(gid,-1): continue
            latest[gid]=record['retrieved_at']
            # Replace rather than union superseded box snapshots.
            evidence[gid]={}
            for team in game['teams']:
                tid=games[gid].get(team['team'])
                if tid is None: raise ValueError('Cached box team mismatch')
                for category in team['categories']:
                    for stat in category['types']:
                        for athlete in stat['athletes']:
                            key=(gid,str(athlete['id']),tid)
                            if key not in wanted or not positive_activity(category['name'],stat['name'],athlete['stat']): continue
                            e=evidence[gid].setdefault(key,{'activity':[],'cache':path.name,'retrieved_at':record['retrieved_at']})
                            e['activity'].append(f"{category['name']}:{stat['name']}")
    flat={key:value for per_game in evidence.values() for key,value in per_game.items()}
    for key,events in play_evidence.items():
        flat.setdefault(key,{'activity':[]})['play_events']=events
    return flat,set(latest)


def reconcile(team, category, sums):
    if team is None or sums is None: return False
    if category=='passing': volume,yards=team['pass_attempts'],team['pass_yards']
    elif category=='rushing': volume,yards=team['rush_attempts'],team['rush_yards']
    else:
        raw={s['category']:s['stat'] for s in team['payload']['stats']}
        try: volume=int(raw['completionAttempts'].split('-')[0])
        except (KeyError,ValueError,AttributeError): return False
        yards=team['pass_yards']
    return volume is not None and yards is not None and sums==(volume,yards)


def classify(actual_yards,actual_volume,activity,reconciled):
    if actual_yards is not None:
        if actual_yards==0:
            return 'explicit_zero_yards',actual_yards,None
        return 'observed_yards',actual_yards,None
    if activity and reconciled:
        return 'inferred_zero_sensitivity_only',None,0
    if activity: return 'unresolved_with_activity',None,None
    return 'unresolved_no_positive_evidence',None,None


def recommendation_gate(*, availability, availability_observed_at, cutoff,
                        identity_verified, distribution_validated, grading_rules_verified):
    """Fail closed; postgame audit evidence cannot satisfy pregame availability."""
    reasons=[]
    if availability!='confirmed': reasons.append('availability_unconfirmed')
    known_at=pd.to_datetime(availability_observed_at,utc=True,errors='coerce')
    as_of=pd.to_datetime(cutoff,utc=True,errors='coerce')
    if pd.isna(known_at) or pd.isna(as_of) or known_at>as_of:
        reasons.append('availability_not_known_at_cutoff')
    if identity_verified is not True: reasons.append('player_identity_unverified')
    if distribution_validated is not True: reasons.append('distribution_not_validated')
    if grading_rules_verified is not True: reasons.append('grading_rules_unverified')
    return {'allowed':not reasons,'reasons':reasons}


def run(run_ids):
    run_ids=sorted(set(run_ids))
    with connect() as conn:
        source=conn.execute('SELECT id,kind FROM cfb_model_v1.prediction_runs WHERE id=ANY(%s)',(run_ids,)).fetchall()
        if {r[0] for r in source}!=set(run_ids) or any(r[1]!='player_backtest' for r in source):
            raise ValueError('Specify existing player backtest run IDs only')
        predictions=conn.execute('''SELECT run_id,game_id,player_id,team_id,category,predicted_yards,baseline_yards,actual_volume,actual_yards
        FROM cfb_model_v1.player_predictions WHERE run_id=ANY(%s)''',(run_ids,)).fetchall()
        game_ids=sorted({r[1] for r in predictions})
        if not game_ids: raise ValueError('No predictions to audit')
        games={r[0]:{r[3]:r[1],r[4]:r[2]} for r in conn.execute('SELECT game_id,home_id,away_id,home_team,away_team FROM cfb_model_v1.games WHERE game_id=ANY(%s)',(game_ids,)).fetchall()}
        rows=conn.execute('SELECT game_id,team_id,pass_yards,pass_attempts,rush_yards,rush_attempts,payload FROM cfb_model_v1.team_game_stats WHERE game_id=ANY(%s)',(game_ids,)).fetchall()
        teams={(r[0],r[1]):dict(zip(['pass_yards','pass_attempts','rush_yards','rush_attempts','payload'],r[2:])) for r in rows}
        sums={(r[0],r[1],r[2]):(int(r[3]),int(r[4])) for r in conn.execute('''SELECT game_id,team_id,category,sum(volume),sum(yards)
        FROM cfb_model_v1.player_offense WHERE game_id=ANY(%s) GROUP BY game_id,team_id,category''',(game_ids,)).fetchall()}
    wanted={(r[1],r[2],r[3]) for r in predictions}
    evidence,covered=evidence_from_cache(games,wanted)
    records=[]
    for rid,gid,pid,tid,category,pred,baseline,volume,yards in predictions:
        e=dict(evidence.get((gid,pid,tid),{'activity':[]}))
        e['box_snapshot_available']=gid in covered
        e['team_totals_reconcile']=reconcile(teams.get((gid,tid)),category,sums.get((gid,tid,category)))
        e['postgame_only']=True
        has_activity=bool(e['activity'] or e.get('play_events'))
        status,verified,sensitivity=classify(yards,volume,has_activity,e['team_totals_reconcile'])
        e['source_outcome_volume']=volume
        records.append({'source_run_id':rid,'game_id':gid,'player_id':pid,'team_id':tid,'category':category,
                        'status':status,'verified_yards':verified,'sensitivity_yards':sensitivity,'evidence':e,
                        'predicted_yards':pred,'baseline_yards':baseline})
    frame=pd.DataFrame(records)
    report=[]
    for category,group in frame.groupby('category'):
        verified=group.dropna(subset=['verified_yards'])
        expanded=group.copy(); expanded['sensitivity_outcome']=expanded.verified_yards.fillna(expanded.sensitivity_yards)
        expanded=expanded.dropna(subset=['sensitivity_outcome'])
        report.append({'category':category,'candidates':len(group),'statuses':group.status.value_counts().to_dict(),
                       'verified_rows':len(verified),'verified_model_mae':float((verified.predicted_yards-verified.verified_yards).abs().mean()),
                       'verified_baseline_mae':float((verified.baseline_yards-verified.verified_yards).abs().mean()),
                       'sensitivity_rows':len(expanded),'sensitivity_model_mae':float((expanded.predicted_yards-expanded.sensitivity_outcome).abs().mean()),
                       'sensitivity_baseline_mae':float((expanded.baseline_yards-expanded.sensitivity_outcome).abs().mean())})
    config={'source_run_ids':run_ids,'policy':'Explicit source outcomes only are verified. Activity plus exact team volume/yard reconciliation allows sensitivity-only zero; never a DNP classification.',
            'no_training_changes':True,'postgame_evidence_not_pregame_features':True}
    with connect() as conn:
        audit_id=conn.execute('INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id',
                              ('pou-outcome-audit-v0','postgame_audit',json.dumps(config),json.dumps(report))).fetchone()[0]
        with conn.pipeline():
            for r in records:
                conn.execute('INSERT INTO cfb_model_v1.player_outcome_audits VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                             (audit_id,r['source_run_id'],r['game_id'],r['player_id'],r['team_id'],r['category'],r['status'],
                              r['verified_yards'],r['sensitivity_yards'],json.dumps(r['evidence'])))
    out=ROOT/'outputs'/f'outcome_audit_{audit_id}'; out.mkdir(parents=True,exist_ok=True)
    (out/'summary.json').write_text(json.dumps(report,indent=2))
    (out/'configuration.json').write_text(json.dumps(config,indent=2))
    frame.drop(columns=['evidence']).to_csv(out/'outcomes.csv',index=False)
    print(json.dumps({'audit_run_id':audit_id,'results':report,'output':str(out)},indent=2),flush=True)
