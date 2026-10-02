"""Walk-forward baseline using jointly estimated offensive/defensive ratings.

Score-based first baseline, not the eventual passing/rushing efficiency model.
Ratings are refitted using only games from earlier UTC dates. The supervised
mapping is trained only on previous seasons' walk-forward feature rows.

Margin and total are read off two separately regularized fits. Margin is a
difference of four coefficients, where shrinkage pulls every matchup toward a
coin flip; total is a sum, which benefits from much heavier shrinkage. Sharing
one alpha forced a compromise that was wrong for both. Recency decays on days
rather than whole seasons, so a week-13 game no longer counts the same as a
week-1 game within the same season. See RESULTS.md for the measured effect.
"""
import json
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error
from cfb.data import ROOT, connect

MARGIN_ALPHA = 0.2
TOTAL_ALPHA = 20.0
HALF_LIFE_DAYS = 240.0
LOOKBACK_SEASONS = 3
MIN_PAST_GAMES = 100
RATING_COLUMNS = ['game_id', 'season', 'kickoff', 'margin_rating', 'total_rating',
                  'home_field', 'margin', 'total']
RATING_CONFIG = {'margin_alpha': MARGIN_ALPHA, 'total_alpha': TOTAL_ALPHA,
                 'half_life_days': HALF_LIFE_DAYS, 'lookback_seasons': LOOKBACK_SEASONS,
                 'decay': 'exponential on days before the prediction date',
                 'training_cutoff': 'prior UTC date'}


def _design(past, lookup, n, day, half_life_days):
    """Two scoring rows per game: each team's points, its opponent's defense, signed HFA."""
    m = len(past)
    home = past.home_id.map(lookup).to_numpy()
    away = past.away_id.map(lookup).to_numpy()
    field = np.where(past.neutral_site.to_numpy(), 0.0, 1.0)
    index = np.arange(m)
    rows = np.concatenate([index, index, index, index + m, index + m, index + m])
    cols = np.concatenate([home, n + away, np.full(m, 2 * n), away, n + home, np.full(m, 2 * n)])
    vals = np.concatenate([np.ones(m), np.ones(m), field, np.ones(m), np.ones(m), -field])
    x = csr_matrix((vals, (rows, cols)), shape=(2 * m, 2 * n + 1))
    y = np.concatenate([past.home_points.to_numpy(float), past.away_points.to_numpy(float)])
    age = np.array([(day - d).days for d in past.date], dtype=float)
    weight = 0.5 ** (age / half_life_days)
    return x, y, np.concatenate([weight, weight])


def rating_features(games, prediction_ids=None, margin_alpha=MARGIN_ALPHA,
                    total_alpha=TOTAL_ALPHA, half_life_days=HALF_LIFE_DAYS,
                    lookback_seasons=LOOKBACK_SEASONS):
    ids = sorted(set(games.home_id) | set(games.away_id))
    lookup = {t: i for i, t in enumerate(ids)}
    n = len(ids)
    records = []
    games = games.sort_values('kickoff').copy()
    games['date'] = pd.to_datetime(games.kickoff, utc=True).dt.date
    for day, slate in games.groupby('date', sort=True):
        if prediction_ids is not None and not slate.game_id.isin(prediction_ids).any():
            continue
        if prediction_ids is not None:
            slate = slate[slate.game_id.isin(prediction_ids)]
        year = int(slate.season.max())
        past = games[(games.date < day) & (games.season >= year - lookback_seasons)]
        if len(past) < MIN_PAST_GAMES:
            continue
        x, y, weights = _design(past, lookup, n, day, half_life_days)
        fits = {}
        for name, alpha in (('margin', margin_alpha), ('total', total_alpha)):
            model = Ridge(alpha=alpha, solver='lsqr', tol=1e-8).fit(x, y, sample_weight=weights)
            fits[name] = (model.coef_, float(model.intercept_))
        for r in slate.itertuples():
            hi, ai = lookup[r.home_id], lookup[r.away_id]
            field = 0 if r.neutral_site else 1
            scored = {}
            for name, (coef, intercept) in fits.items():
                home = intercept + coef[hi] + coef[n + ai] + field * coef[-1]
                away = intercept + coef[ai] + coef[n + hi] - field * coef[-1]
                scored[name] = (home, away)
            records.append({'game_id': r.game_id, 'season': r.season, 'kickoff': r.kickoff,
                            'margin_rating': scored['margin'][0] - scored['margin'][1],
                            'total_rating': scored['total'][0] + scored['total'][1],
                            'home_field': field,
                            'margin': r.home_points - r.away_points,
                            'total': r.home_points + r.away_points})
    # A shaped empty frame keeps downstream merges working before enough history
    # exists, so callers reach their own insufficient-history path instead of KeyError.
    return pd.DataFrame(records, columns=RATING_COLUMNS)


def market_consensus(game_ids=None):
    """Median closing spread/total per game. Home-perspective margin; the benchmark."""
    query = '''SELECT game_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY spread) spread,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY over_under) total
        FROM cfb_model_v1.betting_lines GROUP BY game_id'''
    with connect() as conn:
        rows = conn.execute(query).fetchall()
    frame = pd.DataFrame(rows, columns=['game_id', 'spread', 'total_line'])
    frame['market_margin'] = -frame.spread
    if game_ids is not None:
        frame = frame[frame.game_id.isin(set(game_ids))]
    return frame


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
    # The closing line is the standard any game model is actually judged against.
    market = market_consensus(test.game_id).dropna(subset=['market_margin'])
    if not market.empty:
        graded = test.merge(market, on='game_id')
        metrics['market_benchmark'] = {
            'games_with_line': len(graded),
            'market_margin_mae': float(mean_absolute_error(graded.margin, graded.market_margin)),
            'market_winner_accuracy': float(((graded.market_margin > 0) == (graded.margin > 0)).mean()),
            'model_margin_mae_same_games': float(mean_absolute_error(graded.margin, graded.pred_margin)),
            'note': 'Model error above market error means no demonstrated betting edge.'}
    with connect() as conn:
        run_id=conn.execute("INSERT INTO cfb_model_v1.prediction_runs(model_version,kind,configuration,metrics) VALUES (%s,%s,%s,%s) RETURNING id",('score-ridge-v1','historical_backtest',json.dumps({'test_year':test_year,**RATING_CONFIG}),json.dumps(metrics))).fetchone()[0]
        with conn.pipeline():
            for r in test.itertuples():
                cutoff=datetime.combine(r.kickoff.date(),datetime.min.time(),tzinfo=timezone.utc)
                conn.execute('INSERT INTO cfb_model_v1.game_predictions VALUES (%s,%s,%s,%s,%s,%s,%s)',(run_id,r.game_id,cutoff,r.pred_margin,r.pred_total,r.margin,r.total))
    metrics['run_id'] = run_id
    out=ROOT/'outputs'/f'run_{run_id}'; out.mkdir(parents=True,exist_ok=True)
    (out/'baseline_metrics.json').write_text(json.dumps(metrics,indent=2))
    test.to_csv(out/'baseline_predictions.csv',index=False)
    print(json.dumps(metrics,indent=2),flush=True)
