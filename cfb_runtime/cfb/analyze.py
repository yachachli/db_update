"""Local NFL-POU-style analysis contract; never assert unverified availability."""
import hashlib
import json
from datetime import datetime,timezone
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from cfb.data import ROOT,connect
from cfb.pou import candidates
from cfb.backtest import rating_features
from cfb.efficiency import efficiency_features
from cfb.release import predict,probability

STAT={'pass yards':'passing','rush yards':'rushing','rec yards':'receiving'}


def validate_request(request):
    if not isinstance(request,dict): raise ValueError('JSON object required')
    if request.get('stat') not in STAT: raise ValueError('Supported stats: pass yards, rush yards, rec yards')
    try:
        if any(isinstance(request[k],bool) or not str(request[k]).isdigit() for k in ('game_id','player_id')):
            raise ValueError('IDs must be positive integer identifiers')
        if isinstance(request['line'],bool): raise ValueError('Line must be numeric')
        gid=int(request['game_id']); pid=str(int(request['player_id'])); line=float(request['line'])
    except (KeyError,ValueError,TypeError): raise ValueError('Valid game_id, player_id and line required') from None
    if gid<=0 or int(pid)<=0 or not np.isfinite(line) or line<0: raise ValueError('Invalid IDs or line')
    as_of=pd.Timestamp(request.get('as_of') or datetime.now(timezone.utc))
    if as_of.tzinfo is None: raise ValueError('as_of must include a timezone')
    return gid,pid,line,as_of.tz_convert('UTC'),STAT[request['stat']]


def load_artifacts(category):
    root=ROOT/'models/saved/pou_v1'
    manifest=json.loads((root/'manifest.json').read_text())
    entry=manifest['categories'][category]
    paths=[root/entry['artifact'],root/entry['calibration']]
    if any(p.resolve().parent!=root.resolve() for p in paths): raise ValueError('Artifact path outside model directory')
    if hashlib.sha256(paths[0].read_bytes()).hexdigest()!=entry['sha256']: raise ValueError('Model artifact integrity mismatch')
    # Only load our own locally trained, integrity-checked artifacts.
    return manifest,entry,joblib.load(paths[0]),json.loads(paths[1].read_text())


def analyze_internal(request):
    gid,pid,line,as_of,category=validate_request(request)
    manifest,entry,model,cal=load_artifacts(category)
    cutoff=as_of.normalize()
    if pd.Timestamp(cal['last_calibration_kickoff'])>=cutoff: raise ValueError('Model calibration is later than requested cutoff')
    with connect() as conn:
        target=conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_team,away_team,home_classification,away_classification
        FROM cfb_model_v1.games WHERE game_id=%s''',(gid,)).fetchone()
        if target is None: raise ValueError('Unknown game_id; ingest schedule first')
        if as_of>=pd.Timestamp(target[2]): raise ValueError('as_of must be before kickoff; no postgame analysis disguised as pregame')
        if target[1]!=manifest['analysis_season']: raise ValueError('Artifact supports the 2026 season only')
        if target[8:]!=('fbs','fbs'): raise ValueError('This version supports FBS-versus-FBS only')
        history=conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_points,away_points
        FROM cfb_model_v1.games WHERE completed AND kickoff<%s AND season BETWEEN %s AND %s
        AND home_classification='fbs' AND away_classification='fbs' AND home_points IS NOT NULL AND away_points IS NOT NULL ORDER BY kickoff''',
        (cutoff.to_pydatetime(),target[1]-2,target[1])).fetchall()
        games=pd.DataFrame(history,columns=['game_id','season','kickoff','home_id','away_id','neutral_site','home_points','away_points'])
        ids=games.game_id.tolist()
        players=pd.DataFrame(conn.execute('''SELECT game_id,player_id,team_id,player_name,category,volume,yards
        FROM cfb_model_v1.player_offense WHERE game_id=ANY(%s)''',(ids,)).fetchall(),
        columns=['game_id','player_id','team_id','player_name','category','volume','yards'])
        stats=pd.DataFrame(conn.execute('''SELECT game_id,team_id,opponent_id,pass_yards,pass_attempts,rush_yards,rush_attempts
        FROM cfb_model_v1.team_game_stats WHERE game_id=ANY(%s)''',(ids,)).fetchall(),
        columns=['game_id','team_id','opponent_id','pass_yards','pass_attempts','rush_yards','rush_attempts'])
    # Target is only a schedule row: it never contains its actual scores or player outcomes.
    future=pd.DataFrame([dict(zip(games.columns,[*target[:6],np.nan,np.nan]))])
    all_games=pd.concat([games,future],ignore_index=True)
    context=rating_features(all_games,prediction_ids={gid}).merge(
        efficiency_features(all_games,stats,prediction_ids={gid})[0],on='game_id',validate='one_to_one')
    frame=candidates(all_games,players,context)
    selected=frame[(frame.game_id==gid)&(frame.player_id==pid)&(frame.category==category)] if not frame.empty else frame
    canonical_input={**request,'player_id':int(pid),'line':line,
                     'team_code':request.get('team_code','Unknown'),'opponent_abv':request.get('opponent_abv','Unknown')}
    base={'league':'CFB','input':canonical_input,'injury':None,'availability_status':'unknown','grade':0,'over_under':None,
          'recommendation_status':'abstain','market_ready':False,'model_version':manifest['version'],
          'data_cutoff':str(cutoff),'player_position':'Unknown','projected_stat':request['stat'],
          'scope':'Conditional research analysis, not a sportsbook recommendation'}
    if len(selected)!=1:
        return {**base,'projected_value':None,'model_lean':None,'graphs':[],
                'insights':['Insufficient eligible same-team history.','No rookie or new-role projection is fabricated.','Availability and role require confirmation.'],
                'short_answer':'No supported projection for this player/game.','long_answer':'Requires three prior same-season FBS category appearances, recent activity, and minimum opportunity volume.',
                'abstention_reasons':['insufficient_history_or_identity']}
    row=selected.iloc[0]; team=int(row.team_id)
    home=team==target[3]; team_name=target[6] if home else target[7]; opponent=target[7] if home else target[6]
    base['input']={**canonical_input,'team_code':team_name,'opponent_abv':opponent}
    for key,expected in [('team_id',team),('opponent_id',target[4] if home else target[3])]:
        if key in request and (isinstance(request[key],bool) or not str(request[key]).isdigit() or int(request[key])!=expected):
            raise ValueError(f'{key} does not match the game/player history')
    for key,expected in [('team_code',team_name),('opponent_abv',opponent)]:
        if key in request and str(request[key])!=expected: raise ValueError(f'{key} requires the exact CFBD school name in this prototype')
    volume,point=predict(model,selected)
    point=float(point[0]); volume=float(volume[0]); radius=cal['q90']*np.sqrt(max(volume,1))
    probs=probability(point,volume,line,cal['residuals'])
    lean='over' if probs['over']>probs['under'] else 'under' if probs['under']>probs['over'] else None
    recent=players[(players.player_id==pid)&(players.team_id==team)&(players.category==category)].merge(
        games[['game_id','season','kickoff']],on='game_id',validate='many_to_one')
    recent=recent[recent.season==target[1]].sort_values('kickoff').tail(5)
    values=recent.yards.to_numpy()
    reasons=['availability_unconfirmed','sportsbook_identity_unverified','grading_rules_unverified','conditional_distribution_not_market_validated']
    if category=='receiving': reasons.append('receiving_zero_label_sensitivity')
    if not cal['volume_low']<=volume<=cal['volume_high']: reasons.append('outside_calibration_workload')
    if 2*radius>cal['width90_limit']: reasons.append('wide_interval')
    insights=[f"Projects {point:.1f} yards versus a {line:g} line, conditional on a recorded {category} outcome.",
              f"Uses {int(row.prior_games)} prior eligible appearances and opponent-adjusted game context; model: {entry['kind']}.",
              'No bet recommendation: availability, zero-stat coverage, and market grading remain unverified.']
    return {**base,'player_name':row.player_name,'team':team_name,'opponent':opponent,'projected_value':point,
            'projected_volume':volume,'model_lean':lean,'conditional_probabilities':probs,
            'point_target':'conditional median' if entry['kind']=='boosted_median' else 'volume times efficiency',
            'volume_source':'trailing-five mean' if entry['kind']!='volume_efficiency' else 'volume regression',
            'interval90':{'lower':float(point-radius),'upper':float(point+radius),'nominal_level':.9},
            'history':{'games':len(values),'over_hits':int((values>line).sum()),'under_hits':int((values<line).sum()),'pushes':int((values==line).sum())},
            'graphs':[{'version':1,'title':request['stat'],'threshold':line,'data':[{'value':int(r.yards),'label':str(r.game_id),'date':str(r.kickoff)} for r in recent.itertuples()]}],
            'insights':insights,'short_answer':f'Research-only {lean or "neutral"} lean; no recommended bet.',
            'long_answer':' '.join(insights),'abstention_reasons':reasons,
            'validation_run_id':entry['validation_run_id']}


def analyze(request):
    """Public API response, shaped like the NFL POU renderer."""
    from cfb.renderer import render_analysis
    return render_analysis(analyze_internal(request))


def serve(port=8088):
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path!='/cfb_pou': self.send_error(404); return
            try:
                length=int(self.headers.get('Content-Length','0'))
                if not 0<length<=16384: raise ValueError('JSON body must be 1..16384 bytes')
                result=analyze(json.loads(self.rfile.read(length)))
                status=200
            except (ValueError,KeyError,TypeError) as exc:
                result={'error':str(exc)}; status=422
            except Exception:
                # Database/HTTP exception text can contain credentials; never expose it.
                result={'error':'Analysis unavailable; check ingestion and model artifacts.'}; status=503
            payload=json.dumps(result,allow_nan=False).encode()
            self.send_response(status); self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(payload)
        def log_message(self,*args): pass
    print(f'Local CFB API: http://127.0.0.1:{port}/cfb_pou',flush=True)
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('Local server stopped.',flush=True)
    finally:
        server.server_close()
