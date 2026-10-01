"""Append-only box-score observations, not sportsbook settlement or ROI."""
from collections import Counter
import hashlib
import json
import math

from cfb.data import connect
from cfb.markets import timestamp

CATEGORIES = {'player_pass_yds': 'passing', 'player_rush_yds': 'rushing',
              'player_reception_yds': 'receiving'}
STATS = {'player_pass_yds': 'pass yards', 'player_rush_yds': 'rush yards',
         'player_reception_yds': 'rec yards'}


def finite(value):
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def evaluate(row):
    """Pure evaluator. Missing rows never imply zero, a DNP, loss, or void."""
    result = dict(status='pending_game', actual_yards=None, observed_side=None,
                  absolute_error=None, conditional_brier=None, identity_verified=False,
                  sportsbook_settlement='unverified', market_ready=False)
    analysis = row['analysis']
    if row['created_at'] >= min(row['kickoff'], row['provider_kickoff']) or row['captured_at'] >= min(row['kickoff'], row['provider_kickoff']):
        result['status'] = 'invalid_pregame_timing'
        return result
    request = row['request']
    try:
        as_of = timestamp(request['as_of'])
    except (KeyError, TypeError, ValueError, AttributeError):
        result['status'] = 'identity_or_request_mismatch'
        return result
    if (str(request.get('player_id')) != str(row['player_id'])
        or request.get('game_id') != row['game_id']
        or request.get('team_id') != row['team_id']
        or row['market'] not in CATEGORIES
        or request.get('stat') != STATS.get(row['market'])
        or not finite(row['line']) or request.get('line') != row['line']
        or as_of != row['captured_at'] or as_of > row['created_at']):
        result['status'] = 'identity_or_request_mismatch'
        return result
    if not row['completed']:
        return result
    if row['actual_team_id'] is not None and row['actual_team_id'] != row['team_id']:
        result['status'] = 'outcome_team_mismatch'
        return result
    if row['actual_yards'] is None:
        result['status'] = 'missing_box_score_outcome'
        return result
    actual = row['actual_yards']
    if not finite(actual):
        result['status'] = 'invalid_box_score_outcome'
        return result
    result['actual_yards'] = actual
    result['observed_side'] = 'over' if actual > row['line'] else 'under' if actual < row['line'] else 'push'
    point = analysis.get('projected_value')
    if not finite(point):
        result['status'] = 'observed_without_projection'
        return result
    # These are candidate-identity diagnostics, never a record of winning bets.
    result['status'] = 'observed_unverified_identity'
    result['absolute_error'] = abs(actual - point)
    probs = analysis.get('conditional_probabilities', {})
    values = [probs.get(k) for k in ('over', 'under', 'push')]
    if (all(finite(v) and 0 <= v <= 1 for v in values)
        and abs(sum(values) - 1) < 1e-6):
        # Three-class Brier includes pushes, unlike a binary score that drops them.
        result['conditional_brier'] = sum((probs[k] - int(k == result['observed_side'])) ** 2
                                          for k in ('over', 'under', 'push'))
    return result


def summarize(records):
    counts = dict(Counter(r['status'] for r in records))
    errors = [r['absolute_error'] for r in records if r['absolute_error'] is not None]
    briers = [r['conditional_brier'] for r in records if r['conditional_brier'] is not None]
    return dict(total_research_responses=len(records), statuses=counts,
                candidate_identity_projection_count=len(errors),
                candidate_identity_mae=sum(errors)/len(errors) if errors else None,
                candidate_identity_three_class_brier=sum(briers)/len(briers) if briers else None,
                brier_count=len(briers), market_ready=False,
                warning='Conditional box-score diagnostics only; repeated snapshots are correlated. No verified bets or ROI.')


def run():
    with connect() as conn:
        # Serialize this short write transaction so concurrent runs cannot duplicate observations.
        conn.execute('SELECT pg_advisory_xact_lock(719104210)')
        cur = conn.execute('''SELECT f.id AS research_id,f.created_at,f.request,f.analysis,
            c.captured_at,c.kickoff AS provider_kickoff,c.game_id,c.match_status AS game_match_status,
            q.player_id,q.team_id,q.market,q.line,q.match_status AS player_match_status,
            g.kickoff,g.completed,p.team_id AS actual_team_id,p.yards AS actual_yards
            FROM cfb_model_v1.forward_research f
            JOIN cfb_model_v1.prop_quotes q ON q.id=f.quote_id
            JOIN cfb_model_v1.prop_captures c ON c.id=q.capture_id
            JOIN cfb_model_v1.games g ON g.game_id=c.game_id
            LEFT JOIN cfb_model_v1.player_offense p ON p.game_id=c.game_id AND p.player_id=q.player_id
              AND p.category=CASE q.market WHEN 'player_pass_yds' THEN 'passing'
                 WHEN 'player_rush_yds' THEN 'rushing' WHEN 'player_reception_yds' THEN 'receiving' END
            ORDER BY f.id''')
        rows = [dict(zip([d.name for d in cur.description], row)) for row in cur.fetchall()]
        records = []
        for row in rows:
            result = evaluate(row)
            # Include source evidence, so later stat/schedule corrections append a new observation.
            evidence = dict(result=result, kickoff=row['kickoff'].isoformat(), completed=row['completed'],
                            actual_team_id=row['actual_team_id'], actual_yards=row['actual_yards'])
            digest = hashlib.sha256(json.dumps(evidence, sort_keys=True, allow_nan=False).encode()).hexdigest()
            latest = conn.execute('''SELECT evidence_sha256 FROM cfb_model_v1.forward_outcomes
                WHERE research_id=%s ORDER BY id DESC LIMIT 1''', (row['research_id'],)).fetchone()
            if latest is None or latest[0] != digest:
                conn.execute('''INSERT INTO cfb_model_v1.forward_outcomes(research_id,status,details,evidence_sha256)
                    VALUES (%s,%s,%s,%s)''',
                    (row['research_id'], result['status'], json.dumps(evidence, allow_nan=False), digest))
            records.append(result)
    report = summarize(records)
    print(json.dumps(report, indent=2, allow_nan=False))
    return report
