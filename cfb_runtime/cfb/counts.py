"""Chronologically selected count models and discrete residual calibration.

Count artifacts are separate from yardage artifacts. No historical sportsbook
lines or outcomes are invented; validation lines are explicitly synthetic.
"""
import hashlib
import json
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from cfb.pou import COLS
from cfb.intervals import residual_quantile

COUNT_STATS = {'pass attempts': 'passing', 'rush carries': 'rushing', 'receptions': 'receiving'}
COUNT_MARKETS = {'player_pass_attempts': 'pass attempts',
                 'player_rush_attempts': 'rush carries', 'player_receptions': 'receptions'}


def fit(kind, frame):
    if kind == 'count_trailing5':
        return {'kind': kind}
    if kind != 'count_poisson':
        raise ValueError('Unknown count estimator')
    model = HistGradientBoostingRegressor(loss='poisson', max_iter=150,
        max_leaf_nodes=15, min_samples_leaf=80, l2_regularization=10,
        learning_rate=.05, early_stopping=False, random_state=42)
    model.fit(frame[COLS], frame.actual_volume)
    return {'kind': kind, 'estimator': model}


def predict(model, frame):
    if model['kind'] == 'count_trailing5':
        values = frame.volume_mean.to_numpy(float)
    elif model['kind'] == 'count_poisson':
        values = model['estimator'].predict(frame[COLS])
    else:
        raise ValueError('Not a count artifact')
    if not np.isfinite(values).all():
        raise ValueError('Nonfinite count prediction')
    return np.maximum(0, values)


def draws(point, residuals):
    residuals = np.asarray(residuals, dtype=float)
    if (not np.isfinite(point) or point < 0 or residuals.ndim != 1
            or len(residuals) < 100 or not np.isfinite(residuals).all()):
        raise ValueError('Invalid count calibration')
    return np.maximum(0, np.rint(point + residuals * np.sqrt(max(point, 1))))


def probability(point, line, residuals):
    if not np.isfinite(line) or line < 0:
        raise ValueError('Invalid count line')
    samples = draws(point, residuals)
    # Empirical three-class mass preserves impossible-negative-count boundaries.
    return {key: float(mask.mean()) for key, mask in
            [('over', samples > line), ('under', samples < line), ('push', samples == line)]}


def split(group, year):
    observed = group.dropna(subset=['actual_volume'])
    train = observed[observed.season <= year - 2]
    calibration = observed[observed.season == year - 1]
    test = observed[observed.season == year]
    if min(len(train), len(calibration), len(test)) < 100:
        raise ValueError('Insufficient chronological count data')
    if (train.kickoff.max() >= calibration.data_cutoff.min()
            or calibration.kickoff.max() >= test.data_cutoff.min()):
        raise ValueError('Count calibration overlaps forecast cutoff')
    return train, calibration, test


def train(features, output):
    """Train offline; add count entries only after all markets pass validation.

    Existing yardage and participation artifacts must already be in output.
    Does not write to Neon, fetch odds, or promote a production model.
    """
    features, output = Path(features), Path(output)
    manifest_path = output / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    frame = pd.read_csv(features, dtype={'player_id': str}, parse_dates=['kickoff', 'data_cutoff'])
    known = frame.actual_volume.dropna().to_numpy(float)
    if not np.isfinite(known).all() or (known < 0).any() or (known != np.floor(known)).any():
        raise ValueError('Count labels must be nonnegative integers')
    entries, reports = {}, []
    source_hash = hashlib.sha256(features.read_bytes()).hexdigest()
    for stat, category in COUNT_STATS.items():
        group = frame[frame.category == category]
        observed = group.dropna(subset=['actual_volume'])
        development = observed[observed.season <= 2022]
        validation = observed[observed.season == 2023]
        if min(len(development), len(validation)) < 100:
            raise ValueError('Insufficient count selection data')
        if development.kickoff.max() >= validation.data_cutoff.min():
            raise ValueError('Count selection overlaps forecast cutoff')
        scores = {kind: float(mean_absolute_error(validation.actual_volume,
                    predict(fit(kind, development), validation)))
                  for kind in ('count_trailing5', 'count_poisson')}
        chosen = ('count_poisson' if scores['count_poisson'] < .98 * scores['count_trailing5']
                  else 'count_trailing5')
        for year in (2025, 2026):
            training, calibration, test = split(group, year)
            model = fit(chosen, training)
            cp = predict(model, calibration)
            residuals = (calibration.actual_volume.to_numpy() - cp) / np.sqrt(np.maximum(cp, 1))
            q = residual_quantile(np.abs(residuals), .9)
            pred = predict(model, test)
            lines = np.floor(test.volume_mean.to_numpy()) + .5
            probs = np.array([probability(p, l, residuals)['over'] for p, l in zip(pred, lines)])
            actual = test.actual_volume.to_numpy()
            lo = np.maximum(0, pred - q * np.sqrt(np.maximum(pred, 1)))
            hi = pred + q * np.sqrt(np.maximum(pred, 1))
            metrics = {'stat': stat, 'year': year, 'kind': chosen, 'selection_2023_mae': scores,
                'training_rows': len(training), 'calibration_rows': len(calibration), 'test_rows': len(test),
                'model_mae': float(mean_absolute_error(actual, pred)),
                'baseline_mae': float(mean_absolute_error(actual, test.volume_mean)),
                'coverage90': float(((actual >= lo) & (actual <= hi)).mean()),
                'synthetic_line_brier': float(np.mean((probs - (actual > lines)) ** 2)),
                'features_sha256': source_hash,
                'scope': 'Conditional recorded-category counts; synthetic reference lines, not sportsbook performance'}
            reports.append(metrics)
            print(json.dumps(metrics), flush=True)
            if year == 2026:
                key = category + '_volume'
                artifact = output / (key + '_building.joblib')
                joblib.dump(model, artifact)
                artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
                destination = output / (key + '_' + artifact_hash[:16] + '.joblib')
                artifact.replace(destination)
                artifact = destination
                cal = output / (key + '_building_calibration.json')
                cal.write_text(json.dumps({'target': 'volume', 'category': category,
                    'residuals': residuals.tolist(), 'q90': q, 'n': len(calibration),
                    'volume_low': float(np.quantile(cp, .01)), 'volume_high': float(np.quantile(cp, .99)),
                    'width90_limit': float(np.quantile(2*q*np.sqrt(np.maximum(cp, 1)), .9)),
                    'last_calibration_kickoff': str(calibration.kickoff.max())}))
                cal_hash = hashlib.sha256(cal.read_bytes()).hexdigest()
                destination = output / (key + '_' + cal_hash[:16] + '_calibration.json')
                cal.replace(destination)
                cal = destination
                base = manifest['categories'][category]
                entries[key] = {'kind': chosen, 'target': 'volume', 'stat': stat,
                    'model_version': 'cfb-counts-v1', 'artifact': artifact.name,
                    'sha256': hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    'calibration': cal.name, 'calibration_sha256': hashlib.sha256(cal.read_bytes()).hexdigest(),
                    'validation_run_id': 'offline-' + source_hash[:12] + '-' + key,
                    'participation_artifact': base['participation_artifact'],
                    'participation_sha256': base['participation_sha256'], 'validation': metrics}
    manifest['categories'].update(entries)
    manifest['count_release'] = {'version':'cfb-counts-v1','features_sha256':source_hash}
    temp = output / 'manifest.json.tmp'
    temp.write_text(json.dumps(manifest, indent=2) + '\n')
    temp.replace(manifest_path)
    (output / 'count_validation.json').write_text(json.dumps(reports, indent=2) + '\n')
    return reports
