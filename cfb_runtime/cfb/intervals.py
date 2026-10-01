"""Frozen-model chronological residual intervals; no betting probabilities.

Nominal levels are evaluated empirically. Correlated football observations,
seasonal shifts and category-observation selection preclude an IID guarantee.
"""
import json
import math
import numpy as np
import pandas as pd
from cfb.data import ROOT,connect
from cfb.pou import predict_snapshot
from cfb.outcomes import recommendation_gate

LEVELS=(.8,.9)
METHODS=('absolute','volume_scaled')


def residual_quantile(scores,level):
    scores=np.asarray(scores,float)
    if not 0<level<1 or len(scores)==0 or not np.isfinite(scores).all() or (scores<0).any():
        raise ValueError('Invalid conformity scores or level')
    rank=math.ceil((len(scores)+1)*level)
    return float('inf') if rank>len(scores) else float(np.sort(scores)[rank-1])


def scale(volume,method):
    if method not in METHODS: raise ValueError('Unknown interval method')
    values=np.asarray(volume,float)
    if not np.isfinite(values).all(): raise ValueError('Non-finite predicted volume')
    return np.ones(len(values)) if method=='absolute' else np.sqrt(np.maximum(1,values))


def calibrate(y,pred,volume,method):
    y,pred,volume=map(lambda x:np.asarray(x,float),(y,pred,volume))
    if len(y)<100 or len(y)!=len(pred) or len(y)!=len(volume):
        raise ValueError('At least 100 aligned calibration outcomes required')
    scores=np.abs(y-pred)/scale(volume,method)
    quantiles={str(level):residual_quantile(scores,level) for level in LEVELS}
    widths=2*quantiles['0.9']*scale(volume,method)
    return {'method':method,'quantiles':quantiles,'n':len(y),
            'width90_limit':float(np.quantile(widths,.9)),
            'volume_low':float(np.quantile(volume,.01)),'volume_high':float(np.quantile(volume,.99))}


def bounds(calibration,pred,volume,level):
    pred=np.asarray(pred,float)
    if not np.isfinite(pred).all(): raise ValueError('Non-finite point forecast')
    radius=calibration['quantiles'][str(level)]*scale(volume,calibration['method'])
    return pred-radius,pred+radius


def research_reasons(calibration,volume,category):
    # This function has no test-label argument by design.
    width90=2*calibration['quantiles']['0.9']*scale([volume],calibration['method'])[0]
    reasons=[]
    if category=='receiving': reasons.append('receiving_zero_label_sensitivity')
    if not np.isfinite(width90) or width90>calibration['width90_limit']+1e-9:
        reasons.append('unusually_wide_interval')
    if not calibration['volume_low']<=volume<=calibration['volume_high']:
        reasons.append('volume_outside_calibration_support')
    return reasons


def evaluate(out):
    observed=out.dropna(subset=['actual_yards'])
    def summary(rows):
        if rows.empty: return {'n':0,'coverage':None,'mean_width':None}
        return {'n':len(rows),'coverage':float(((rows.actual_yards>=rows.lower_yards)&(rows.actual_yards<=rows.upper_yards)).mean()),
                'mean_width':float((rows.upper_yards-rows.lower_yards).mean())}
    return {'candidates':len(out),'ungraded':int(out.actual_yards.isna().sum()),
            'research_eligible_candidates':int(out.research_eligible.sum()),
            'market_eligible_candidates':0,'observed':summary(observed),
            'research_eligible_observed':summary(observed[observed.research_eligible]),
            'negative_lower_fraction':float((out.lower_yards<0).mean())}


def validate_split(cal,test,cal_year,test_year):
    if test_year!=cal_year+1: raise ValueError('Calibration must immediately precede test season')
    if set(cal.season)!={cal_year} or set(test.season)!={test_year}: raise ValueError('Source season mismatch')
    if pd.to_datetime(cal.kickoff,utc=True).max()>=pd.to_datetime(test.data_cutoff,utc=True).min():
        raise ValueError('Calibration outcomes overlap test prediction cutoff')


def run(run_ids):
    run_ids=sorted(set(run_ids))
    with connect() as conn:
        raw=conn.execute('SELECT id,model_version,kind,configuration FROM cfb_model_v1.prediction_runs WHERE id=ANY(%s)',(run_ids,)).fetchall()
        if {r[0] for r in raw}!=set(run_ids): raise ValueError('Missing source run')
        sources={}
        for rid,version,kind,config in raw:
            if version!='pou-volume-efficiency-v0' or kind!='player_backtest' or 'model_parameters' not in config:
                raise ValueError('Only saved v0 player models supported')
            key=(config['test_year'],config['category'])
            if key in sources: raise ValueError('Ambiguous duplicate season/category run')
            sources[key]=(rid,config)
        records=conn.execute('''SELECT p.run_id,p.game_id,p.player_id,p.category,p.data_cutoff,p.predicted_yards,
        p.actual_yards,p.features,g.season,g.kickoff FROM cfb_model_v1.player_predictions p
        JOIN cfb_model_v1.games g USING(game_id) WHERE p.run_id=ANY(%s)''',(run_ids,)).fetchall()
    data=pd.DataFrame(records,columns=['source_run_id','game_id','player_id','category','data_cutoff','stored_prediction','actual_yards','features','season','kickoff'])
    summary=[]
    for (test_year,category),(test_id,_) in sorted(sources.items()):
        if (test_year-1,category) not in sources: continue
        cal_id,config=sources[(test_year-1,category)]
        cal=data[data.source_run_id==cal_id].copy(); test=data[data.source_run_id==test_id].copy()
        validate_split(cal,test,test_year-1,test_year)
        bundle=config['model_parameters']
        cal_volume,cal_pred=predict_snapshot(bundle,pd.DataFrame(cal.features.tolist()))
        test_volume,test_pred=predict_snapshot(bundle,pd.DataFrame(test.features.tolist()))
        if not np.allclose(cal_pred,cal.stored_prediction.to_numpy(),atol=1e-8,rtol=1e-9):
            raise ValueError('Saved calibration model does not reproduce its original forecasts')
        known=cal.actual_yards.notna().to_numpy()
        # No outcomes from the test season enter calibration, scaling or abstention thresholds.
        for method in METHODS:
            calibration=calibrate(cal.actual_yards.to_numpy()[known],cal_pred[known],cal_volume[known],method)
            for level in LEVELS:
                out=test[['source_run_id','game_id','player_id','category','actual_yards']].copy()
                out['predicted_yards']=test_pred
                out['lower_yards'],out['upper_yards']=bounds(calibration,test_pred,test_volume,level)
                reasons=[research_reasons(calibration,v,category) for v in test_volume]
                out['research_eligible']=[not r for r in reasons]
                market_reasons=recommendation_gate(availability='unknown',availability_observed_at=None,
                    cutoff=test.data_cutoff.min(),identity_verified=False,distribution_validated=False,grading_rules_verified=False)['reasons']
                out['abstention_reasons']=[{'research':r,'market':market_reasons} for r in reasons]
                metrics=evaluate(out)
                configuration={'model_source_run':cal_id,'test_feature_source_run':test_id,'category':category,
                    'model_training_through':test_year-2,'calibration_season':test_year-1,'test_season':test_year,
                    'level':level,'calibration':calibration,'frozen_model_parameters':bundle,
                    'scope':'Explicit category outcomes only; no exchangeability or individual conditional-coverage guarantee',
                    'abstention':'Receiving experimental; width90 above calibration 90th percentile; volume outside calibration 1st–99th percentile',
                    'all_market_recommendations_blocked':True}
                with connect() as conn:
                    rid=conn.execute('INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id',
                                     ('pou-interval-v0','interval_backtest',json.dumps(configuration),json.dumps(metrics))).fetchone()[0]
                    with conn.pipeline():
                        for r in out.itertuples():
                            conn.execute('INSERT INTO cfb_model_v1.player_intervals VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                         (rid,r.source_run_id,r.game_id,r.player_id,r.category,r.predicted_yards,r.lower_yards,r.upper_yards,
                                          None if pd.isna(r.actual_yards) else int(r.actual_yards),r.research_eligible,True,json.dumps(r.abstention_reasons)))
                path=ROOT/'outputs'/f'run_{rid}'; path.mkdir(parents=True,exist_ok=True)
                out.to_csv(path/'intervals.csv',index=False)
                (path/'configuration.json').write_text(json.dumps(configuration,indent=2))
                (path/'metrics.json').write_text(json.dumps(metrics,indent=2))
                row={'run_id':rid,'test_year':test_year,'category':category,'method':method,'level':level,**metrics}
                summary.append(row); print(json.dumps(row),flush=True)
    if not summary: raise ValueError('Provide at least two consecutive seasons of source models')
    path=ROOT/'outputs'/f'interval_comparison_{summary[-1]["run_id"]}'; path.mkdir(parents=True,exist_ok=True)
    (path/'summary.json').write_text(json.dumps(summary,indent=2))
    print(f'Interval comparison: {path}',flush=True)
