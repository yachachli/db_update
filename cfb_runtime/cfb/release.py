"""Fixed development selection, chronological validation and reusable POU artifacts."""
import json
import hashlib
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import brier_score_loss,mean_absolute_error,roc_auc_score
from cfb.pou import COLS,fit_predict,participation_model,predict_snapshot
from cfb.intervals import residual_quantile
from cfb.data import ROOT,connect

KINDS=('trailing5','volume_efficiency','boosted_median')


def fit(kind,train):
    if kind=='trailing5': return {'kind':kind}
    if kind=='volume_efficiency':
        _,_,bundle=fit_predict(train,train.iloc[:1])
        return {'kind':kind,'parameters':bundle}
    if kind!='boosted_median': raise ValueError('Unsupported candidate')
    model=HistGradientBoostingRegressor(loss='absolute_error',max_iter=150,max_leaf_nodes=15,
        min_samples_leaf=80,l2_regularization=10,learning_rate=.05,early_stopping=False,random_state=42)
    model.fit(train[COLS],train.actual_yards)
    return {'kind':kind,'estimator':model}


def predict(model,frame):
    if model['kind']=='volume_efficiency': return predict_snapshot(model['parameters'],frame)
    volume=frame.volume_mean.to_numpy(float)
    yards=frame.yards_mean.to_numpy(float) if model['kind']=='trailing5' else model['estimator'].predict(frame[COLS])
    return volume,yards


def probability(point,volume,line,residuals):
    residuals=np.asarray(residuals,float)
    if not np.isfinite([point,volume,line]).all() or volume<0 or len(residuals)<100 or not np.isfinite(residuals).all():
        raise ValueError('Invalid probability inputs')
    # Yardage is discrete; integer lines have a separate push probability.
    draws=np.rint(point+np.asarray(residuals)*np.sqrt(max(volume,1)))
    n=len(draws)
    return {'over':float(((draws>line).sum()+.5)/(n+1)),
            'under':float(((draws<line).sum()+.5)/(n+1)),
            'push':float((draws==line).sum()/(n+1))}


def selection(scores):
    baseline=scores['trailing5']
    best=min(KINDS,key=lambda k:scores[k])
    return best if scores[best]<=.98*baseline else 'trailing5'


def train():
    path=ROOT/'outputs/candidate_features_through_2026.csv'
    frame=pd.read_csv(path,dtype={'player_id':str},parse_dates=['kickoff','data_cutoff'])
    output=ROOT/'models/saved/pou_v1'; output.mkdir(parents=True,exist_ok=True)
    manifest={'version':'cfb-pou-v2','selection_season':2023,'calibration_season':2025,'analysis_season':2026,
              'population':'prior-history FBS category candidates, history carried across seasons on the same team',
              'projection':'projected_value is conditional on a recorded category outcome; '
                           'unconditional_projected_value multiplies it by the participation probability',
              'market_ready':False,'categories':{}}
    results=[]
    for category in ('passing','rushing','receiving'):
        group=frame[frame.category==category]
        observed=group.dropna(subset=['actual_yards','actual_volume'])
        development=observed[observed.season<=2022]; validation=observed[observed.season==2023]
        if len(development)<100 or len(validation)<100: raise ValueError('Insufficient selection data')
        scores={kind:float(mean_absolute_error(validation.actual_yards,predict(fit(kind,development),validation)[1])) for kind in KINDS}
        chosen=selection(scores)
        print(json.dumps({'category':category,'selection_2023_mae':scores,'chosen':chosen}),flush=True)
        for year in (2025,2026):
            training=observed[observed.season<=year-2]
            calibration=observed[observed.season==year-1]
            test=group[group.season==year].copy()
            if calibration.kickoff.max()>=test.data_cutoff.min(): raise ValueError('Calibration overlaps forecast cutoff')
            model=fit(chosen,training)
            # Participation trains on every candidate, not just graded rows: the
            # absences are the label. Same chronological cutoff as the yardage model.
            play=participation_model(group[group.season<=year-2])
            test_play=play.predict_proba(test[COLS])[:,1]
            cv,cp=predict(model,calibration); volume,point=predict(model,test)
            residuals=(calibration.actual_yards.to_numpy()-cp)/np.sqrt(np.maximum(cv,1))
            q=residual_quantile(np.abs(residuals),.9)
            test['participation_probability']=test_play
            test['unconditional_prediction']=test_play*point
            test['prediction']=point
            test['lower90']=point-q*np.sqrt(np.maximum(volume,1)); test['upper90']=point+q*np.sqrt(np.maximum(volume,1))
            test['reference_line']=np.floor(test.yards_mean)+.5
            test['over_probability']=[probability(p,v,l,residuals)['over'] for p,v,l in zip(point,volume,test.reference_line)]
            known=test.dropna(subset=['actual_yards'])
            y=(known.actual_yards>known.reference_line).astype(float)
            cal_rate=float((calibration.actual_yards>np.floor(calibration.yards_mean)+.5).mean())
            bins=[]
            for low in np.arange(0,1,.1):
                mask=(known.over_probability>=low)&(known.over_probability<low+.1)
                if mask.any(): bins.append({'low':float(low),'n':int(mask.sum()),'predicted':float(known.loc[mask,'over_probability'].mean()),'observed':float(y[mask].mean())})
            metrics={'year':year,'category':category,'chosen':chosen,'selection_mae':scores,
                     'candidate_rows':len(test),'graded_rows':len(known),'ungraded_rows':int(test.actual_yards.isna().sum()),
                     'model_mae':float(mean_absolute_error(known.actual_yards,known.prediction)),
                     'baseline_mae':float(mean_absolute_error(known.actual_yards,known.yards_mean)),
                     'coverage90':float(((known.actual_yards>=known.lower90)&(known.actual_yards<=known.upper90)).mean()),
                     'mean_width90':float((known.upper90-known.lower90).mean()),
                     'participation_rate':float(test.participated.mean()),
                     'participation_auc':float(roc_auc_score(test.participated,test_play))
                        if test.participated.nunique()>1 else None,
                     'participation_brier':float(brier_score_loss(test.participated,test_play)),
                     'participation_base_rate_brier':float(test.participated.mean()*(1-test.participated.mean())),
                     'zero_filled_conditional_mae':float(mean_absolute_error(test.actual_yards.fillna(0.0),point)),
                     'zero_filled_unconditional_mae':float(mean_absolute_error(test.actual_yards.fillna(0.0),test.unconditional_prediction)),
                     'synthetic_line_brier':float(np.mean((known.over_probability-y)**2)),
                     'calibration_base_rate_brier':float(np.mean((cal_rate-y)**2)),
                     'reliability_bins':bins,'market_ready':False,
                     'warning':'Synthetic pregame reference lines, NOT historical sportsbook quotes; conditional outcome population'}
            config={'selection_year':2023,'training_through':year-2,'calibration_year':year-1,'test_year':year,
                    'candidate_kinds':KINDS,'minimum_development_improvement':.02,'features':COLS,
                    'fixed_boosting_parameters':{'loss':'absolute_error','max_iter':150,'max_leaf_nodes':15,'min_samples_leaf':80,
                        'l2_regularization':10,'learning_rate':.05,'early_stopping':False,'random_state':42},
                    'no_tuning_on_2025_or_2026':True,'line_rule':'floor(trailing5_yards_mean)+0.5'}
            with connect() as conn:
                rid=conn.execute('INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id',
                    ('cfb-pou-v2','release_validation',json.dumps(config),json.dumps(metrics))).fetchone()[0]
                with conn.pipeline():
                    for r in test.itertuples():
                        conn.execute('INSERT INTO cfb_model_v1.release_predictions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                            (rid,r.game_id,r.player_id,category,r.prediction,r.lower90,r.upper90,r.reference_line,r.over_probability,
                             None if pd.isna(r.actual_yards) else int(r.actual_yards),True))
            metrics['run_id']=rid; results.append(metrics)
            out=ROOT/'outputs'/f'run_{rid}'; out.mkdir(parents=True,exist_ok=True)
            test.to_csv(out/'release_predictions.csv',index=False)
            (out/'metrics.json').write_text(json.dumps(metrics,indent=2))
            print(json.dumps({k:v for k,v in metrics.items() if k not in ('reliability_bins','selection_mae')}),flush=True)
            if year==2026:
                artifact=output/f'{category}_{rid}.joblib'
                joblib.dump(model,artifact)
                play_artifact=output/f'{category}_{rid}_participation.joblib'
                joblib.dump(play,play_artifact)
                calibration_path=output/f'{category}_{rid}_calibration.json'
                calibration_path.write_text(json.dumps({'residuals':residuals.tolist(),'q90':q,'n':len(calibration),
                    'volume_low':float(np.quantile(cv,.01)),'volume_high':float(np.quantile(cv,.99)),
                    'width90_limit':float(np.quantile(2*q*np.sqrt(np.maximum(cv,1)),.9)),
                    'last_calibration_kickoff':str(calibration.kickoff.max())}))
                manifest['categories'][category]={'kind':chosen,'artifact':artifact.name,'sha256':hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    'calibration':calibration_path.name,'validation_run_id':rid,'experimental':category=='receiving',
                    'participation_artifact':play_artifact.name,
                    'participation_sha256':hashlib.sha256(play_artifact.read_bytes()).hexdigest(),
                    'participation_auc':metrics['participation_auc']}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2))
    (ROOT/'outputs/release_validation.json').write_text(json.dumps(results,indent=2))
    print(f'Reusable artifacts: {output}',flush=True)
