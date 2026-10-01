"""Opponent-adjusted game-level advanced metrics with prior-date cutoffs."""
import json
import math
import numpy as np
import pandas as pd
from cfb.data import Client, ROOT, connect
from cfb.backtest import rating_features
from cfb.efficiency import BASE, fit_ratings, evaluate

METRICS = ['pass_success','rush_success','pass_explosiveness','rush_explosiveness','plays']
FEATURES = [f'{side}_{metric}_{unit}' for side in ('home','away')
            for metric in METRICS for unit in ('offense','allowance')]


def normalize(clean, full):
    offense = clean['offense']
    values = []
    for key in ('successRate','explosiveness'):
        for kind in ('passingPlays','rushingPlays'):
            value = offense.get(kind,{}).get(key)
            if value is not None:
                value = float(value)
                if not math.isfinite(value) or (key=='successRate' and not 0<=value<=1):
                    raise ValueError('Invalid advanced metric')
            values.append(value)
    for value in (offense.get('plays'), full['offense'].get('plays')):
        if value is not None and (int(value)!=value or value<0):
            raise ValueError('Invalid play count')
        values.append(value)
    return values


def ingest(years):
    client = Client(max_calls=40)
    for year in years:
        with connect() as conn:
            rows = conn.execute('SELECT game_id,home_id,away_id,home_team,away_team FROM cfb_model_v1.games WHERE season=%s AND completed',(year,)).fetchall()
        games = {r[0]:r for r in rows}
        params = {'year':year,'seasonType':'both'}
        clean = client.get('/stats/game/advanced',{**params,'excludeGarbageTime':'true'})
        full = client.get('/stats/game/advanced',{**params,'excludeGarbageTime':'false'})
        full_map = {(r['gameId'],r['team']):r for r in full}
        if len(full_map)!=len(full): raise ValueError('Duplicate full advanced team rows')
        seen=set(); stored=0; outside=0
        with connect() as conn, conn.pipeline():
            for r in clean:
                if r['season']!=year: raise ValueError('Wrong advanced-stat season')
                gid=r['gameId']
                if gid not in games:
                    outside+=1
                    continue  # Endpoint includes games outside our FBS-involved warehouse.
                g=games[gid]; names={g[3]:g[1],g[4]:g[2]}
                if r['team'] not in names or r['opponent'] not in names or r['team']==r['opponent']:
                    raise ValueError('Advanced-stat team mapping mismatch')
                key=(gid,r['team'])
                if key in seen or key not in full_map: raise ValueError('Duplicate/unpaired advanced row')
                seen.add(key); raw=full_map[key]
                if raw['opponent']!=r['opponent']: raise ValueError('Paired opponent mismatch')
                conn.execute('''INSERT INTO cfb_model_v1.advanced_game_stats
                (game_id,team_id,opponent_id,pass_success,rush_success,pass_explosiveness,rush_explosiveness,clean_plays,plays,clean_payload,full_payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(game_id,team_id) DO UPDATE SET pass_success=excluded.pass_success,rush_success=excluded.rush_success,
                pass_explosiveness=excluded.pass_explosiveness,rush_explosiveness=excluded.rush_explosiveness,
                clean_plays=excluded.clean_plays,plays=excluded.plays,clean_payload=excluded.clean_payload,
                full_payload=excluded.full_payload,retrieved_at=now()''',
                (gid,names[r['team']],names[r['opponent']],*normalize(r,raw),json.dumps(r),json.dumps(raw)))
                stored+=1
        print(json.dumps({'season':year,'advanced_rows':stored,'outside_schedule_rows':outside}),flush=True)


def features(games, stats):
    games=games.copy()
    games['date']=pd.to_datetime(games.kickoff,utc=True).dt.normalize()
    stats=stats.merge(games[['game_id','season','date','home_id','neutral_site']],on='game_id',validate='many_to_one')
    stats['home_field']=np.where(stats.neutral_site,0,np.where(stats.team_id==stats.home_id,1,-1))
    records=[]
    for day,slate in games.groupby('date',sort=True):
        year=int(slate.season.max())
        past=stats[(stats.date<day)&(stats.season>=year-2)]
        fits={}
        for metric in METRICS:
            data=past.copy()
            # Equal game weights for volume; total clean-play exposure proxy for split metrics.
            # The endpoint does not supply separate pass/rush or successful-play counts.
            exposure = 30.0 if metric=='plays' else data.clean_plays/2
            data['value_attempts']=exposure
            data['value_yards']=data[metric]*exposure
            fits[metric]=fit_ratings(data,'value',year)[0]
        for r in slate.itertuples():
            row={'game_id':r.game_id}
            for side in ('home','away'):
                for metric in METRICS:
                    values=fits[metric].get(getattr(r,f'{side}_id'),{})
                    for unit in ('offense','allowance'):
                        row[f'{side}_{metric}_{unit}']=values.get(unit,0.0)
            records.append(row)
    return pd.DataFrame(records)


def coverage_report(games, stats, allow_missing_games=False):
    counts=stats.groupby('game_id').size().reindex(games.game_id,fill_value=0)
    missing_games=sorted(map(int,counts[counts!=2].index))
    if missing_games and (not allow_missing_games or len(missing_games)/len(games)>.01):
        raise ValueError(f'Missing advanced rows for {len(missing_games)} games; inspect before --allow-missing-games (1% maximum)')
    missing=stats[stats[METRICS+['clean_plays']].isna().any(axis=1)]
    if len(missing): raise ValueError(f'Missing metric values in {len(missing)} rows; inspect before training')
    if not np.isfinite(stats[METRICS+['clean_plays']].to_numpy(float)).all():
        raise ValueError('Non-finite advanced metrics')
    return {'team_rows':len(stats),'missing_game_ids':missing_games,'explicit_missing_override':allow_missing_games,
            'policy':'Missing advanced observations excluded from rating fits; all scoring targets retained'}


def run(years, allow_missing_games=False):
    years=sorted(set(years))
    with connect() as conn:
        games=pd.DataFrame(conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_points,away_points
        FROM cfb_model_v1.games WHERE completed AND home_points IS NOT NULL AND away_points IS NOT NULL
        AND home_classification='fbs' AND away_classification='fbs' AND season<=%s ORDER BY kickoff''',(max(years),)).fetchall(),
        columns=['game_id','season','kickoff','home_id','away_id','neutral_site','home_points','away_points'])
        stats=pd.DataFrame(conn.execute('''SELECT a.game_id,team_id,opponent_id,pass_success,rush_success,pass_explosiveness,
        rush_explosiveness,clean_plays,plays FROM cfb_model_v1.advanced_game_stats a JOIN cfb_model_v1.games g USING(game_id)
        WHERE g.completed AND g.home_classification='fbs' AND g.away_classification='fbs' AND g.season<=%s''',(max(years),)).fetchall(),
        columns=['game_id','team_id','opponent_id',*METRICS[:-1],'clean_plays','plays'])
    if games.empty or stats.empty: raise ValueError('Ingest advanced stats first')
    quality=coverage_report(games,stats,allow_missing_games)
    print(json.dumps({'coverage':quality}),flush=True)
    base=rating_features(games)
    frame=base.merge(features(games,stats),on='game_id',validate='one_to_one')
    summary=[]
    for year in years:
        train,test=frame[frame.season<year],frame[frame.season==year]
        if len(train)<100 or test.empty: raise ValueError('Insufficient chronological data')
        for version,cols in [('score-ridge-v0',BASE),('advanced-ridge-v2',BASE+FEATURES)]:
            predictions,metrics=evaluate(train,test,cols,version!='score-ridge-v0')
            config={'test_year':year,'features':cols,'rating_alpha':12,'mapping_alpha':10,'decay':.45,
                    'cutoff':'prior UTC date','garbage_time':'excluded for efficiency, included for volume',
                    'weights':'clean total plays/60 for split metrics; one per game for volume',
                    'scope':'FBS versus FBS','data_quality':quality}
            with connect() as conn:
                rid=conn.execute('INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id',
                                 (version,'historical_backtest',json.dumps(config),json.dumps(metrics))).fetchone()[0]
                with conn.pipeline():
                    for r in predictions.itertuples():
                        cutoff=pd.Timestamp(r.kickoff).tz_convert('UTC').normalize().to_pydatetime()
                        conn.execute('INSERT INTO cfb_model_v1.game_predictions VALUES (%s,%s,%s,%s,%s,%s,%s)',
                                     (rid,r.game_id,cutoff,r.pred_margin,r.pred_total,r.margin,r.total))
                    for _,r in test.iterrows():
                        conn.execute('INSERT INTO cfb_model_v1.game_features VALUES (%s,%s,%s,%s)',
                                     (rid,int(r.game_id),pd.Timestamp(r.kickoff).normalize().to_pydatetime(),json.dumps({c:float(r[c]) for c in cols})))
            out=ROOT/'outputs'/f'run_{rid}'; out.mkdir(parents=True,exist_ok=True)
            predictions.to_csv(out/'predictions.csv',index=False)
            (out/'configuration.json').write_text(json.dumps(config,indent=2))
            (out/'metrics.json').write_text(json.dumps(metrics,indent=2))
            summary.append({'year':year,'model':version,'run_id':rid,**metrics})
            print(json.dumps(summary[-1]),flush=True)
    out=ROOT/'outputs'/f'advanced_comparison_{summary[-1]["run_id"]}'; out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(summary).to_csv(out/'comparison.csv',index=False)
    (out/'data_quality.json').write_text(json.dumps(quality,indent=2))
    print(f'Comparison: {out}',flush=True)
