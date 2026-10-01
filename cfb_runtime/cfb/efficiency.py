"""Walk-forward opponent-adjusted box-score efficiency, not play-level EPA.

Defense coefficients measure allowance (negative is better). All fits exclude
the prediction date. Attempts weight observations; ridge shrinks sparse teams.
"""
import json
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from cfb.backtest import rating_features
from cfb.data import ROOT, connect

ALPHA = 12.0
DECAY = 0.45
BASE = ['margin_rating', 'total_rating', 'home_field']
EXTRA = [f'{side}_{metric}_{unit}' for side in ('home', 'away')
         for metric in ('pass', 'rush') for unit in ('offense', 'allowance')]


def fit_ratings(past, metric, year, adjusted=True):
    valid = past.dropna(subset=[f'{metric}_yards', f'{metric}_attempts'])
    valid = valid[valid[f'{metric}_attempts'] > 0]
    if valid.empty:
        return {}, 0.0
    ids = sorted(set(valid.team_id) | set(valid.opponent_id))
    lookup = {t:i for i,t in enumerate(ids)}
    n, m = len(ids), len(valid)
    offense = valid.team_id.map(lookup).to_numpy()
    defense = valid.opponent_id.map(lookup).to_numpy()
    attempts = valid[f'{metric}_attempts'].to_numpy(float)
    y = valid[f'{metric}_yards'].to_numpy(float) / attempts
    weights = attempts / 30 * DECAY ** (year - valid.season.to_numpy())
    off_exposure = np.bincount(offense, weights=weights, minlength=n)
    def_exposure = np.bincount(defense, weights=weights, minlength=n)
    if adjusted:
        x = csr_matrix((np.concatenate([np.ones(m), np.ones(m), valid.home_field.to_numpy()]),
                        (np.tile(np.arange(m),3), np.concatenate([offense, n+defense, np.full(m,2*n)]))),
                       shape=(m,2*n+1))
        model = Ridge(alpha=ALPHA, solver='lsqr', tol=1e-8).fit(x,y,sample_weight=weights)
        off, defense_coefs = model.coef_[:n], model.coef_[n:2*n]
        mean = float(model.intercept_)
    else:
        mean = float(np.average(y,weights=weights))
        off = np.bincount(offense,weights=weights*(y-mean),minlength=n)/(off_exposure+ALPHA)
        defense_coefs = np.bincount(defense,weights=weights*(y-mean),minlength=n)/(def_exposure+ALPHA)
    return {team: {'offense':float(off[i]), 'allowance':float(defense_coefs[i]),
                   'off_exposure':float(off_exposure[i]), 'def_exposure':float(def_exposure[i])}
            for team,i in lookup.items()}, mean


def efficiency_features(games, stats, adjusted=True, prediction_ids=None):
    games = games.copy()
    games['date'] = pd.to_datetime(games.kickoff,utc=True).dt.normalize()
    stats = stats.merge(games[['game_id','season','date','home_id','neutral_site']],on='game_id',validate='many_to_one')
    stats['home_field'] = np.where(stats.neutral_site,0,np.where(stats.team_id==stats.home_id,1,-1))
    records, latest = [], []
    for day, slate in games.groupby('date',sort=True):
        if prediction_ids is not None and not slate.game_id.isin(prediction_ids).any():
            continue
        if prediction_ids is not None:
            slate=slate[slate.game_id.isin(prediction_ids)]
        year = int(slate.season.max())
        past = stats[(stats.date < day) & (stats.season >= year-2)]
        fits = {metric:fit_ratings(past,metric,year,adjusted)[0] for metric in ('pass','rush')}
        for r in slate.itertuples():
            record = {'game_id':r.game_id}
            for side in ('home','away'):
                for metric in ('pass','rush'):
                    values = fits[metric].get(getattr(r,f'{side}_id'),{})
                    for unit in ('offense','allowance'):
                        record[f'{side}_{metric}_{unit}'] = values.get(unit,0.0)
            records.append(record)
        latest = [{'data_cutoff':str(day),'team_id':team,'metric':metric,**values}
                  for metric,fit in fits.items() for team,values in fit.items()]
    return pd.DataFrame(records), pd.DataFrame(latest)


def bracket(percentile):
    if percentile >= .9: return 'top_decile'
    if percentile >= .7: return 'above_average'
    if percentile >= .3: return 'middle'
    if percentile >= .1: return 'below_average'
    return 'bottom_decile'


def evaluate(train, test, cols, standardized):
    predictions = test[['game_id','kickoff','margin','total']].copy()
    metrics = {'train_games':len(train),'test_games':len(test)}
    for target in ('margin','total'):
        model = make_pipeline(StandardScaler(),Ridge(alpha=10)) if standardized else Ridge(alpha=10)
        model.fit(train[cols],train[target])
        p = model.predict(test[cols])
        predictions[f'pred_{target}'] = p
        metrics[f'{target}_mae'] = float(mean_absolute_error(test[target],p))
        metrics[f'{target}_rmse'] = float(np.sqrt(mean_squared_error(test[target],p)))
    metrics['winner_accuracy'] = float(((predictions.pred_margin>0)==(predictions.margin>0)).mean())
    return predictions, metrics


def run(years, allow_missing_stats=False):
    years = sorted(set(years))
    with connect() as conn:
        games = pd.DataFrame(conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_points,away_points,
        home_team,away_team FROM cfb_model_v1.games WHERE completed AND home_points IS NOT NULL AND away_points IS NOT NULL
        AND home_classification='fbs' AND away_classification='fbs' AND season<=%s ORDER BY kickoff''',(max(years),)).fetchall(),
        columns=['game_id','season','kickoff','home_id','away_id','neutral_site','home_points','away_points','home_team','away_team'])
        stats = pd.DataFrame(conn.execute('''SELECT s.game_id,team_id,opponent_id,pass_yards,pass_attempts,rush_yards,rush_attempts
        FROM cfb_model_v1.team_game_stats s JOIN cfb_model_v1.games g USING(game_id)
        WHERE g.season<=%s AND g.completed AND g.home_classification='fbs' AND g.away_classification='fbs' ''',(max(years),)).fetchall(),
        columns=['game_id','team_id','opponent_id','pass_yards','pass_attempts','rush_yards','rush_attempts'])
    if games.empty or stats.empty:
        raise ValueError('Ingest games and team stats first')
    coverage = stats.groupby('game_id').size().reindex(games.game_id,fill_value=0)
    if (coverage != 2).any():
        raise ValueError(f'Incomplete team-stat coverage: {(coverage != 2).sum()} games; inspect ingestion before training')
    missing = stats[stats[['pass_yards','pass_attempts','rush_yards','rush_attempts']].isna().any(axis=1)]
    if not missing.empty and (not allow_missing_stats or len(missing)/len(stats) > .01):
        raise ValueError(f'{len(missing)} team rows have missing efficiency stats; inspect before using --allow-missing-stats (1% maximum)')
    quality = {'team_rows':len(stats),'missing_team_rows':len(missing),
               'missing_game_ids':sorted(map(int,missing.game_id.unique())),
               'policy':'Exclude missing observations only from relevant efficiency fits; retain all game targets',
               'explicit_missing_override':allow_missing_stats}
    print(json.dumps({'coverage':quality}),flush=True)
    base = rating_features(games)
    adjusted, ranks = efficiency_features(games,stats,True)
    raw, _ = efficiency_features(games,stats,False)
    frames = {'score-ridge-v0':base, 'raw-efficiency-v1':base.merge(raw,on='game_id',validate='one_to_one'),
              'opponent-efficiency-v1':base.merge(adjusted,on='game_id',validate='one_to_one')}
    summary=[]
    for year in years:
        for version, frame in frames.items():
            train, test = frame[frame.season<year],frame[frame.season==year]
            if len(train)<100 or test.empty: raise ValueError('Insufficient chronological fold data')
            cols = BASE if version=='score-ridge-v0' else BASE+EXTRA
            predictions,metrics = evaluate(train,test,cols,version!='score-ridge-v0')
            config = {'test_year':year,'features':cols,'alpha':ALPHA,'decay':DECAY,
                      'cutoff':'prior UTC date','scope':'FBS versus FBS; final box scores include overtime',
                      'standardized':version!='score-ridge-v0','mapping_alpha':10,'data_quality':quality}
            with connect() as conn:
                run_id=conn.execute('''INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics)
                VALUES (%s,%s,%s,%s) RETURNING id''',(version,'historical_backtest',json.dumps(config),json.dumps(metrics))).fetchone()[0]
                with conn.pipeline():
                    for r in predictions.itertuples():
                        cutoff=pd.Timestamp(r.kickoff).tz_convert('UTC').normalize().to_pydatetime()
                        conn.execute('INSERT INTO cfb_model_v1.game_predictions VALUES (%s,%s,%s,%s,%s,%s,%s)',
                                     (run_id,r.game_id,cutoff,r.pred_margin,r.pred_total,r.margin,r.total))
                    for _, r in test.iterrows():
                        conn.execute('INSERT INTO cfb_model_v1.game_features VALUES (%s,%s,%s,%s)',
                                     (run_id,int(r.game_id),pd.Timestamp(r.kickoff).normalize().to_pydatetime(),json.dumps({c:float(r[c]) for c in cols})))
            out=ROOT/'outputs'/f'run_{run_id}'; out.mkdir(parents=True,exist_ok=True)
            predictions.to_csv(out/'predictions.csv',index=False)
            (out/'metrics.json').write_text(json.dumps(metrics,indent=2))
            (out/'configuration.json').write_text(json.dumps(config,indent=2))
            summary.append({'year':year,'model':version,'run_id':run_id,**metrics})
            print(json.dumps(summary[-1]),flush=True)
    out=ROOT/'outputs'/f'efficiency_comparison_{summary[-1]["run_id"]}'
    out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(summary).to_csv(out/'comparison.csv',index=False)
    (out/'data_quality.json').write_text(json.dumps(quality,indent=2))
    names=dict(zip(games.home_id,games.home_team)); names.update(zip(games.away_id,games.away_team))
    ranks['team']=ranks.team_id.map(names)
    for unit in ('offense','allowance'):
        strength = ranks[unit] if unit=='offense' else -ranks[unit]
        ranks[unit+'_percentile'] = strength.groupby(ranks.metric).rank(pct=True,method='average')
        ranks[unit+'_bracket'] = ranks[unit+'_percentile'].map(bracket)
        exposure = ranks['off_exposure' if unit=='offense' else 'def_exposure']
        ranks.loc[exposure<3,unit+'_bracket']='insufficient_sample'
    ranks.to_csv(out/'historical_ratings.csv',index=False)
    print(f'Comparison and historical ratings: {out}',flush=True)
