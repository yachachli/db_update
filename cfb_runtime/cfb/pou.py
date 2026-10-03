"""Player point projections plus an explicit participation model.

Eligibility carries a player's history across seasons on the same team. Keying
history to a single season blacked out the opening weeks every year, including
for returning starters with a full prior season on record. Cross-season history
roughly doubles candidate coverage and also lowers error on the rows that were
already eligible, so the extra coverage is not bought with accuracy.

Roughly half of all candidates never record a stat in their category, so a
conditional yardage projection alone is mis-specified for the population it is
applied to. participation_model estimates that probability directly; the
unconditional projection is the product. Neither is an availability claim: the
label is "a box score recorded this category", not "the player was healthy".
"""
import json
import hashlib
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import brier_score_loss, mean_absolute_error, mean_squared_error, roc_auc_score
from cfb.data import ROOT, connect, copy_rows
from cfb.backtest import rating_features
from cfb.efficiency import efficiency_features

COLS=['volume_mean','volume_last','volume_std','yards_mean','yards_last','efficiency',
      'prior_games','days_since','team_margin','game_total','opponent_allowance',
      'same_season_history','in_season_games']
MIN_VOLUME={'passing':5,'rushing':3,'receiving':1}
MIN_HISTORY=3
SAME_SEASON_MAX_GAP=45
OFFSEASON_MAX_GAP=400
PARTICIPATION_PARAMETERS=dict(loss='log_loss',max_iter=200,max_leaf_nodes=31,min_samples_leaf=50,
                              l2_regularization=1.0,learning_rate=.06,early_stopping=False,random_state=42)


def candidates(games, observations, context):
    """Enumerate from history before revealing each day's observation rows."""
    games=games.copy(); games['date']=pd.to_datetime(games.kickoff,utc=True).dt.normalize()
    observations=observations.merge(games[['game_id','date','season']],on='game_id',validate='many_to_one')
    by_day={day:g for day,g in observations.groupby('date')}
    target={(int(r.game_id),str(r.player_id),r.category):r for r in observations.itertuples()}
    ctx=context.set_index('game_id').to_dict('index')
    history=defaultdict(lambda:defaultdict(list)); rows=[]
    # Carrying history across seasons means a transfer would otherwise leave a
    # player enumerated for their old team too. Only their latest team counts.
    current_team={}
    for day,slate in games.groupby('date',sort=True):
        for g in slate.itertuples():
            if g.game_id not in ctx: continue
            game_context=ctx[g.game_id]
            for side,opposite in [('home','away'),('away','home')]:
                team_id=getattr(g,f'{side}_id')
                for category in MIN_VOLUME:
                    # History follows the player's team, not the season, so a
                    # returning starter is eligible in week one.
                    for pid,records in history[(team_id,category)].items():
                        if len(records)<MIN_HISTORY: continue
                        if current_team.get((pid,category))!=team_id: continue
                        past=records[-5:]
                        gap=(day-past[-1]['date']).days
                        same_season=past[-1]['season']==g.season
                        if gap>(SAME_SEASON_MAX_GAP if same_season else OFFSEASON_MAX_GAP): continue
                        volumes=np.array([r['volume'] for r in past],float)
                        yards=np.array([r['yards'] for r in past],float)
                        if volumes.mean()<MIN_VOLUME[category]: continue
                        metric='rush' if category=='rushing' else 'pass'
                        row={'game_id':g.game_id,'kickoff':g.kickoff,'data_cutoff':day,'season':g.season,
                             'team_id':team_id,'player_id':pid,'player_name':past[-1]['player_name'],'category':category,
                             'volume_mean':float(volumes.mean()),'volume_last':float(volumes[-1]),'volume_std':float(volumes.std()),
                             'yards_mean':float(yards.mean()),'yards_last':float(yards[-1]),
                             'efficiency':float(yards.sum()/volumes.sum()),'prior_games':len(records),'days_since':gap,
                             'team_margin':game_context['margin_rating']*(1 if side=='home' else -1),
                             'game_total':game_context['total_rating'],
                             'opponent_allowance':game_context[f'{opposite}_{metric}_allowance'],
                             'same_season_history':float(same_season),
                             'in_season_games':float(sum(r['season']==g.season for r in records))}
                        # Joining observed outcomes happens only after eligibility/features are determined.
                        observed=target.get((g.game_id,pid,category))
                        row['actual_volume']=observed.volume if observed is not None else np.nan
                        row['actual_yards']=observed.yards if observed is not None else np.nan
                        # Label for the participation model: a recorded category
                        # outcome, which is not the same as confirmed availability.
                        row['participated']=float(observed is not None)
                        rows.append(row)
        if day in by_day:
            for r in by_day[day].itertuples():
                history[(r.team_id,r.category)][str(r.player_id)].append(
                    {'date':day,'season':r.season,'volume':r.volume,'yards':r.yards,'player_name':r.player_name})
                current_team[(str(r.player_id),r.category)]=r.team_id
    return pd.DataFrame(rows)


def participation_model(train):
    """P(the box score records this category). Not availability, not an injury model."""
    if len(train)<200 or train.participated.nunique()<2:
        raise ValueError('Insufficient participation training rows')
    model=HistGradientBoostingClassifier(**PARTICIPATION_PARAMETERS)
    model.fit(train[COLS],train.participated)
    return model


def fit_predict(train,test):
    model=make_pipeline(StandardScaler(),Ridge(alpha=10))
    model.fit(train[COLS],train.actual_volume)
    positive=train[train.actual_volume>0]
    if len(positive)<100: raise ValueError('Insufficient positive-volume efficiency training rows')
    efficiency=make_pipeline(StandardScaler(),Ridge(alpha=10))
    efficiency.fit(positive[COLS],positive.actual_yards/positive.actual_volume,
                   ridge__sample_weight=positive.actual_volume/positive.actual_volume.mean())
    volume=np.maximum(0,model.predict(test[COLS]))
    def snapshot(pipeline):
        scaler=pipeline.named_steps['standardscaler']; ridge=pipeline.named_steps['ridge']
        return {'mean':scaler.mean_.tolist(),'scale':scaler.scale_.tolist(),
                'coefficients':ridge.coef_.tolist(),'intercept':float(ridge.intercept_)}
    bundle={'features':COLS,'volume':snapshot(model),'efficiency':snapshot(efficiency)}
    return volume,volume*efficiency.predict(test[COLS]),bundle


def predict_snapshot(bundle, frame):
    x=frame[bundle['features']].to_numpy(float)
    def predict(part):
        return ((x-np.array(part['mean']))/np.array(part['scale'])) @ np.array(part['coefficients'])+part['intercept']
    volume=np.maximum(0,predict(bundle['volume']))
    return volume,volume*predict(bundle['efficiency'])


def run(years,allow_missing_games=False):
    years=sorted(set(years))
    with connect() as conn:
        games=pd.DataFrame(conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,neutral_site,home_points,away_points
        FROM cfb_model_v1.games WHERE completed AND home_points IS NOT NULL AND away_points IS NOT NULL
        AND home_classification='fbs' AND away_classification='fbs' AND season<=%s ORDER BY kickoff''',(max(years),)).fetchall(),
        columns=['game_id','season','kickoff','home_id','away_id','neutral_site','home_points','away_points'])
        players=pd.DataFrame(conn.execute('''SELECT p.game_id,player_id,team_id,player_name,category,volume,yards
        FROM cfb_model_v1.player_offense p JOIN cfb_model_v1.games g USING(game_id)
        WHERE g.completed AND g.home_classification='fbs' AND g.away_classification='fbs' AND g.season<=%s''',(max(years),)).fetchall(),
        columns=['game_id','player_id','team_id','player_name','category','volume','yards'])
        stats=pd.DataFrame(conn.execute('''SELECT s.game_id,team_id,opponent_id,pass_yards,pass_attempts,rush_yards,rush_attempts
        FROM cfb_model_v1.team_game_stats s JOIN cfb_model_v1.games g USING(game_id)
        WHERE g.completed AND g.home_classification='fbs' AND g.away_classification='fbs' AND g.season<=%s''',(max(years),)).fetchall(),
        columns=['game_id','team_id','opponent_id','pass_yards','pass_attempts','rush_yards','rush_attempts'])
    if games.empty or players.empty: raise ValueError('Ingest player history first')
    missing_games=sorted(map(int,set(games.game_id)-set(players.game_id)))
    expected_teams={(int(r.game_id),int(t)) for r in games.itertuples() for t in (r.home_id,r.away_id)}
    observed_teams=set(zip(players.game_id.astype(int),players.team_id.astype(int)))
    missing_teams=sorted(expected_teams-observed_teams)
    affected_games={gid for gid,_ in missing_teams}
    if affected_games and (not allow_missing_games or len(affected_games)/len(games)>.01):
        raise ValueError(f'Incomplete offensive player history for {len(affected_games)} games; audit before --allow-missing-games (1% maximum)')
    quality={'missing_offensive_game_ids':missing_games,'explicit_missing_override':allow_missing_games,
             'missing_offensive_team_games':[{'game_id':g,'team_id':t} for g,t in missing_teams],
             'observed_player_category_rows':len(players),'scope':'FBS versus FBS through final test season'}
    print(json.dumps({'coverage':quality}),flush=True)
    context=rating_features(games).merge(efficiency_features(games,stats)[0],on='game_id',validate='one_to_one')
    frame=candidates(games,players,context)
    if frame.empty: raise ValueError('No eligible prior-history candidates')
    feature_path=ROOT/'outputs'/f'candidate_features_through_{max(years)}.csv'
    feature_path.parent.mkdir(parents=True,exist_ok=True)
    frame.to_csv(feature_path,index=False)
    summary=[]
    for year in years:
        for category in MIN_VOLUME:
            train=frame[(frame.season<year)&(frame.category==category)].dropna(subset=['actual_yards','actual_volume'])
            test=frame[(frame.season==year)&(frame.category==category)].copy()
            if len(train)<100 or test.empty: raise ValueError('Insufficient chronological player data')
            volume,yards,bundle=fit_predict(train,test)
            test['pred_volume'],test['pred_yards']=volume,yards
            # Participation trains on every candidate, including rows with no
            # outcome: those absences are the signal, not missing data.
            participation=frame[(frame.season<year)&(frame.category==category)]
            classifier=participation_model(participation)
            test['participation_probability']=classifier.predict_proba(test[COLS])[:,1]
            test['unconditional_yards']=test.participation_probability*test.pred_yards
            observed=test.dropna(subset=['actual_yards','actual_volume'])
            if observed.empty: raise ValueError('No observed test outcomes')
            metrics={'train_rows':len(train),'candidate_rows':len(test),'graded_rows':len(observed),
                     'ungraded_rows':len(test)-len(observed),
                     'all_observed_category_rows':int(len(players[(players.category==category)&players.game_id.isin(games.loc[games.season==year,'game_id'])])),
                     'yards_mae':float(mean_absolute_error(observed.actual_yards,observed.pred_yards)),
                     'yards_rmse':float(np.sqrt(mean_squared_error(observed.actual_yards,observed.pred_yards))),
                     'trailing5_yards_mae':float(mean_absolute_error(observed.actual_yards,observed.yards_mean)),
                     'last_game_yards_mae':float(mean_absolute_error(observed.actual_yards,observed.yards_last)),
                     'volume_mae':float(mean_absolute_error(observed.actual_volume,observed.pred_volume)),
                     'trailing5_volume_mae':float(mean_absolute_error(observed.actual_volume,observed.volume_mean)),
                     'participation_rate':float(test.participated.mean()),
                     'participation_auc':float(roc_auc_score(test.participated,test.participation_probability))
                        if test.participated.nunique()>1 else None,
                     'participation_brier':float(brier_score_loss(test.participated,test.participation_probability)),
                     'participation_base_rate_brier':float(test.participated.mean()*(1-test.participated.mean())),
                     'zero_filled_conditional_mae':float(mean_absolute_error(test.actual_yards.fillna(0.0),test.pred_yards)),
                     'zero_filled_unconditional_mae':float(mean_absolute_error(test.actual_yards.fillna(0.0),test.unconditional_yards)),
                     'zero_fill_note':'Absent category outcomes scored as zero yards; one grading convention, not a settlement rule'}
            config={'test_year':year,'category':category,'features':COLS,'ridge_alpha':10,
                    'data_quality':quality,
                    'source_sha256':hashlib.sha256((ROOT/'cfb/pou.py').read_bytes()).hexdigest(),
                    'model_parameters':bundle,
                    'eligibility':f'{MIN_HISTORY} prior same-team FBS category appearances carried across seasons; '
                                  f'<={SAME_SEASON_MAX_GAP}d in-season or <={OFFSEASON_MAX_GAP}d across an offseason; trailing-five mean volume threshold',
                    'min_volume':MIN_VOLUME[category],'cutoff':'prior UTC date',
                    'participation_parameters':PARTICIPATION_PARAMETERS,
                    'participation_label':'A box score recorded this category; not confirmed availability',
                    'scope':'Conditional on observed category participation; absent category outcomes remain ungraded',
                    'team_efficiency_missing_game_ids':sorted(map(int,stats.loc[stats[['pass_yards','pass_attempts','rush_yards','rush_attempts']].isna().any(axis=1),'game_id'].unique()))}
            with connect() as conn:
                rid=conn.execute('INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id',
                                 ('pou-volume-efficiency-v1','player_backtest',json.dumps(config),json.dumps(metrics))).fetchone()[0]
                copy_rows(conn,'cfb_model_v1.player_predictions',
                    ['run_id','game_id','player_id','team_id','category','data_cutoff','predicted_volume',
                     'predicted_yards','baseline_yards','actual_volume','actual_yards','features'],
                    [(rid,int(r.game_id),r.player_id,int(r.team_id),category,r.data_cutoff.to_pydatetime(),
                      float(r.pred_volume),float(r.pred_yards),float(r.yards_mean),
                      None if pd.isna(r.actual_volume) else int(r.actual_volume),
                      None if pd.isna(r.actual_yards) else int(r.actual_yards),
                      json.dumps({c:float(r[c]) for c in COLS})) for _,r in test.iterrows()])
            out=ROOT/'outputs'/f'run_{rid}'; out.mkdir(parents=True,exist_ok=True)
            test.to_csv(out/'player_predictions.csv',index=False)
            (out/'configuration.json').write_text(json.dumps(config,indent=2))
            (out/'model_parameters.json').write_text(json.dumps(bundle,indent=2))
            (out/'metrics.json').write_text(json.dumps(metrics,indent=2))
            summary.append({'year':year,'category':category,'run_id':rid,**metrics})
            print(json.dumps(summary[-1]),flush=True)
    out=ROOT/'outputs'/f'pou_comparison_{summary[-1]["run_id"]}'; out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(summary).to_csv(out/'comparison.csv',index=False)
    (out/'data_quality.json').write_text(json.dumps(quality,indent=2))
    print(f'POU comparison: {out}',flush=True)
