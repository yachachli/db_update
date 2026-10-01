"""Walk-forward baseline using jointly estimated offensive/defensive ratings.

Score-based first baseline, not the eventual passing/rushing efficiency model.
Ratings are refitted using only games from earlier UTC dates. The supervised
mapping is trained only on previous seasons' walk-forward feature rows.
"""
import json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from cfb.data import ROOT, connect


def rating_features(games, prediction_ids=None):
    ids = sorted(set(games.home_id) | set(games.away_id))
    lookup = {t:i for i,t in enumerate(ids)}
    n = len(ids)
    records = []
    games = games.sort_values('kickoff').copy()
    games['date'] = pd.to_datetime(games.kickoff, utc=True).dt.date
    for day, slate in games.groupby('date', sort=True):
        if prediction_ids is not None and not slate.game_id.isin(prediction_ids).any():
            continue
        if prediction_ids is not None:
            slate=slate[slate.game_id.isin(prediction_ids)]
        year = int(slate.season.max())
        past = games[(games.date < day) & (games.season >= year - 2)]
        if len(past) < 100:
            continue
        x = np.zeros((len(past)*2, n*2+1))
        y = np.zeros(len(past)*2)
        weights = np.zeros(len(past)*2)
        for j,r in enumerate(past.itertuples()):
            hi,ai = lookup[r.home_id],lookup[r.away_id]
            x[j*2,hi],x[j*2,n+ai] = 1,1
            x[j*2+1,ai],x[j*2+1,n+hi] = 1,1
            x[j*2,-1] = 0 if r.neutral_site else 1
            x[j*2+1,-1] = 0 if r.neutral_site else -1
            y[j*2:j*2+2] = [r.home_points,r.away_points]
            weights[j*2:j*2+2] = 0.45 ** (year-r.season)
        fit = Ridge(alpha=12, solver='lsqr').fit(x,y,sample_weight=weights)
        for r in slate.itertuples():
            hi,ai = lookup[r.home_id],lookup[r.away_id]
            home = fit.intercept_+fit.coef_[hi]+fit.coef_[n+ai]+(0 if r.neutral_site else fit.coef_[-1])
            away = fit.intercept_+fit.coef_[ai]+fit.coef_[n+hi]-(0 if r.neutral_site else fit.coef_[-1])
            records.append({'game_id':r.game_id,'season':r.season,'kickoff':r.kickoff,'margin_rating':home-away,'total_rating':home+away,'home_field':int(not r.neutral_site),'margin':r.home_points-r.away_points,'total':r.home_points+r.away_points})
    return pd.DataFrame(records)


def run(test_year=2025):
    with connect() as conn:
        rows = conn.execute("SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_points,away_points FROM cfb_model_v1.games WHERE completed AND home_points IS NOT NULL AND away_points IS NOT NULL AND home_classification='fbs' AND away_classification='fbs' AND season <= %s ORDER BY kickoff", (test_year,)).fetchall()
    games = pd.DataFrame(rows,columns=['game_id','season','kickoff','home_id','away_id','neutral_site','home_points','away_points'])
    if games.empty:
        raise ValueError('No completed FBS games ingested')
    frame = rating_features(games)
    train,test = frame[frame.season < test_year],frame[frame.season == test_year].copy()
    if len(train)<100 or test.empty:
        raise ValueError('Insufficient chronological training/test data')
    cols=['margin_rating','total_rating','home_field']
    metrics={'train_games':len(train),'test_games':len(test),'test_season':test_year,'scope':'FBS versus FBS; score-only opponent-adjusted baseline; no betting performance claim'}
    for target in ['margin','total']:
        model=Ridge(alpha=10).fit(train[cols],train[target])
        test['pred_'+target]=model.predict(test[cols])
        metrics[target+'_mae']=float(mean_absolute_error(test[target],test['pred_'+target]))
        metrics[target+'_rmse']=float(np.sqrt(mean_squared_error(test[target],test['pred_'+target])))
        metrics[target+'_constant_baseline_mae']=float(mean_absolute_error(test[target],np.repeat(train[target].mean(),len(test))))
    metrics['winner_accuracy']=float(((test.pred_margin>0)==(test.margin>0)).mean())
    with connect() as conn:
        run_id=conn.execute("INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id",('score-ridge-v0','historical_backtest',json.dumps({'test_year':test_year,'rating_alpha':12,'prior_season_decay':0.45,'training_cutoff':'prior UTC date'}),json.dumps(metrics))).fetchone()[0]
        with conn.pipeline():
            for r in test.itertuples():
                cutoff=datetime.combine(r.kickoff.date(),datetime.min.time(),tzinfo=timezone.utc)
                conn.execute('INSERT INTO cfb_model_v1.game_predictions VALUES (%s,%s,%s,%s,%s,%s,%s)',(run_id,r.game_id,cutoff,r.pred_margin,r.pred_total,r.margin,r.total))
    metrics['run_id'] = run_id
    out=ROOT/'outputs'/f'run_{run_id}'; out.mkdir(parents=True,exist_ok=True)
    (out/'baseline_metrics.json').write_text(json.dumps(metrics,indent=2))
    test.to_csv(out/'baseline_predictions.csv',index=False)
    print(json.dumps(metrics,indent=2),flush=True)
