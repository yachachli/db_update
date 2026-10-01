"""Roster corroboration is evidence, not availability or sportsbook ID verification."""
from collections import Counter
from datetime import datetime, timezone
import json
from cfb.data import Client, connect
from cfb.markets import normalize


def audit(quote, game, teams, roster):
    result = dict(status='event_unresolved', player_id=None, team_id=None, position=None,
                  availability_status='unknown', sportsbook_identity_verified=False, market_ready=False)
    for side in ('home', 'away'):
        entries = [t for t in teams if t.get('id') == game[side + '_id']]
        if len(entries) != 1:
            return result
        team = entries[0]
        if team.get('school') != game[side + '_team']:
            return result
        official = normalize(team['school'] + ' ' + (team.get('mascot') or ''))
        if normalize(game['payload'][side + '_team']) != official:
            return result
    if abs((game['kickoff'] - game['provider_kickoff']).total_seconds()) > 900:
        return result
    matches = {(str(p['id']), p['team_id']): p for p in roster
               if normalize(quote['player_name']) == normalize(p['firstName'] + ' ' + p['lastName'])}
    if len(matches) != 1:
        result['status'] = 'roster_name_ambiguous' if matches else 'roster_name_unresolved'
        return result
    (pid, tid), player = next(iter(matches.items()))
    result.update(player_id=pid, team_id=tid, position=player.get('position'))
    if quote['player_id'] is not None and (str(quote['player_id']) != pid or quote['team_id'] != tid):
        result['status'] = 'candidate_conflicts_with_roster'
    else:
        result['status'] = 'roster_corroborated' if quote['player_id'] is not None else 'new_roster_candidate'
    return result


def run(limit=7):
    if not 1 <= limit <= 10:
        raise ValueError('limit must be 1..10 games')
    with connect() as conn:
        cur = conn.execute('''SELECT c.id,c.game_id,c.payload,c.kickoff AS provider_kickoff,
            g.kickoff,g.season,g.home_id,g.away_id,g.home_team,g.away_team
            FROM cfb_model_v1.prop_captures c JOIN cfb_model_v1.games g USING(game_id)
            WHERE c.id=(SELECT max(c2.id) FROM cfb_model_v1.prop_captures c2 WHERE c2.event_id=c.event_id)
            AND EXISTS(SELECT 1 FROM cfb_model_v1.prop_quotes q WHERE q.capture_id=c.id)
            AND g.kickoff>now() AND NOT g.completed ORDER BY g.kickoff,c.id LIMIT %s''', (limit,))
        games = [dict(zip([d.name for d in cur.description], r)) for r in cur.fetchall()]
    client = Client(max_calls=3 * (3 * limit))
    sources = {}
    def source(endpoint, params):
        key = json.dumps([endpoint, params], sort_keys=True)
        if key not in sources:
            payload = client.get(endpoint, params, cache=False)
            with connect() as conn:
                sid = conn.execute('''INSERT INTO cfb_model_v1.identity_sources
                    (observed_at,season,endpoint,parameters,payload) VALUES (%s,%s,%s,%s,%s) RETURNING id''',
                    (datetime.now(timezone.utc), params['year'], endpoint, json.dumps(params), json.dumps(payload))).fetchone()[0]
            sources[key] = (sid, payload)
        return sources[key]
    counts = Counter()
    for game in games:
        tsid, teams = source('/teams/fbs', {'year': game['season']})
        roster, source_ids = [], [tsid]
        for side in ('home', 'away'):
            sid, players = source('/roster', {'year': game['season'], 'team': game[side + '_team']})
            source_ids.append(sid)
            if any(p.get('team') != game[side + '_team'] for p in players):
                raise ValueError('Roster response contains an unexpected team')
            roster.extend({**p, 'team_id': game[side + '_id']} for p in players)
        with connect() as conn:
            cur = conn.execute('SELECT id,player_name,player_id,team_id FROM cfb_model_v1.prop_quotes WHERE capture_id=%s', (game['id'],))
            quotes = [dict(zip([d.name for d in cur.description], r)) for r in cur.fetchall()]
            for quote in quotes:
                evidence = audit(quote, game, teams, roster)
                evidence.update(source_ids=source_ids, game_id=game['game_id'],
                                timing='Evidence retrieved at audit time; not backdated to prediction time')
                conn.execute('''INSERT INTO cfb_model_v1.quote_identity_audits(quote_id,status,evidence)
                    VALUES (%s,%s,%s)''', (quote['id'], evidence['status'], json.dumps(evidence)))
                counts[evidence['status']] += 1
        print(json.dumps({'game_id': game['game_id'], 'audited_quotes': len(quotes)}), flush=True)
    report = dict(games=len(games), statuses=dict(counts), http_calls=client.calls,
                  availability_status='unknown', market_ready=False)
    print(json.dumps(report, indent=2))
    return report
