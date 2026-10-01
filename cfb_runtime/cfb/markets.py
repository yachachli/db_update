"""Bounded live prop collection. Identity suggestions are never verified betting IDs."""
from datetime import datetime, timezone, timedelta
import json
import math
import re
import unicodedata

from cfb.data import Client, connect

MARKETS = {'player_pass_yds': 'pass yards', 'player_rush_yds': 'rush yards',
           'player_reception_yds': 'rec yards'}


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)


def normalize(value):
    # Preserve suffixes and initials; no fuzzy name similarity or Jr/Sr removal.
    return re.sub(r'[^a-z0-9]', '', unicodedata.normalize('NFKD', value).lower())


def event_candidate(event, games):
    """Full school name, optional mascot suffix, same orientation/time. Review required."""
    def school_matches(odds, school):
        return odds.casefold() == school.casefold() or odds.casefold().startswith(school.casefold() + ' ')
    matches = [g for g in games if abs((timestamp(event['commence_time']) - g['kickoff']).total_seconds()) <= 900
               and school_matches(event['home_team'], g['home_team'])
               and school_matches(event['away_team'], g['away_team'])]
    return matches[0] if len(matches) == 1 else None


def player_candidate(name, players):
    matches = {(str(p['player_id']), p['team_id']) for p in players
               if normalize(name) and normalize(name) == normalize(p['player_name'])}
    return next(iter(matches)) if len(matches) == 1 else None


def quotes(event, captured):
    result = []
    for book in event.get('bookmakers', []):
        for market in book.get('markets', []):
            if market['key'] not in MARKETS:
                continue
            try:
                updated = timestamp(market.get('last_update') or book['last_update'])
            except (KeyError, ValueError, TypeError, AttributeError):
                continue
            age = (captured - updated).total_seconds()
            status = 'fresh' if 0 <= age <= 900 else 'stale_or_future'
            for outcome in market.get('outcomes', []):
                try:
                    line, price = float(outcome['point']), float(outcome['price'])
                    name = outcome['description'].strip()
                    if (isinstance(outcome['point'], bool) or isinstance(outcome['price'], bool)
                        or not math.isfinite(line) or not math.isfinite(price) or line < 0
                        or abs(price) < 100 or not price.is_integer() or not name
                        or outcome['name'] not in ('Over', 'Under')):
                        continue
                except (KeyError, TypeError, ValueError, AttributeError):
                    continue
                result.append(dict(bookmaker=book['key'], market=market['key'], player_name=name,
                                   line=line, side=outcome['name'], american_price=int(price),
                                   source_updated_at=updated, quote_status=status))
    return result


def collect(limit=5, analyze_limit=3, new_only=False, per_game_limit=10):
    if not 1 <= limit <= 10 or not 0 <= analyze_limit <= 10:
        raise ValueError('event limit must be 1..10 and analysis limit 0..10')
    if not 1 <= per_game_limit <= 10:
        raise ValueError('per-game analysis limit must be 1..10')
    client = Client('odds', max_calls=1 + 3 * limit)
    base = '/sports/americanfootball_ncaaf/events'
    now = datetime.now(timezone.utc)
    events = client.get(base, cache=False)
    events = sorted([e for e in events if now < timestamp(e['commence_time']) <= now + timedelta(days=7)],
                    key=lambda e: e['commence_time'])[:limit]
    with connect() as conn:
        cur = conn.execute('''SELECT game_id,season,kickoff,home_id,away_id,home_team,away_team
            FROM cfb_model_v1.games WHERE NOT completed AND kickoff>%s AND kickoff<=%s''',
                           (now, now + timedelta(days=7)))
        games = [dict(zip([d.name for d in cur.description], row)) for row in cur.fetchall()]
    summary = {'events': [], 'quotes': 0, 'player_candidates': 0, 'forward_analyses': 0,
               'analysis_errors': 0, 'market_ready': False}
    analyzed = set()
    if new_only:
        with connect() as conn:
            analyzed = set(conn.execute('''SELECT DISTINCT c.game_id,q.player_id,q.market,q.line
                FROM cfb_model_v1.forward_research f
                JOIN cfb_model_v1.prop_quotes q ON q.id=f.quote_id
                JOIN cfb_model_v1.prop_captures c ON c.id=q.capture_id''').fetchall())
    for event in events:
        payload = client.get(base + '/' + event['id'] + '/odds',
                             {'regions': 'us', 'markets': ','.join(MARKETS), 'oddsFormat': 'american'}, cache=False)
        captured = datetime.now(timezone.utc)
        if payload.get('id') != event['id']:
            raise ValueError('Provider event identity changed')
        kickoff = timestamp(payload['commence_time'])
        game = event_candidate(payload, games)
        rows = quotes(payload, captured)
        pending = []
        with connect() as conn:
            capture_id = conn.execute('''INSERT INTO cfb_model_v1.prop_captures
                (captured_at,event_id,kickoff,payload,game_id,match_status) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id''',
                (captured, event['id'], kickoff, json.dumps(payload), game['game_id'] if game else None,
                 'candidate_unverified' if game else 'unresolved')).fetchone()[0]
            players = []
            if game:
                cur = conn.execute('''SELECT DISTINCT p.player_id,p.team_id,p.player_name
                    FROM cfb_model_v1.player_offense p JOIN cfb_model_v1.games g USING(game_id)
                    WHERE g.season=%s AND g.kickoff<%s AND g.completed AND p.team_id=ANY(%s)''',
                    (game['season'], captured.replace(hour=0, minute=0, second=0, microsecond=0),
                     [game['home_id'], game['away_id']]))
                players = [dict(zip([d.name for d in cur.description], row)) for row in cur.fetchall()]
            for row in rows:
                player = player_candidate(row['player_name'], players)
                qid = conn.execute('''INSERT INTO cfb_model_v1.prop_quotes
                    (capture_id,bookmaker,market,player_name,line,side,american_price,source_updated_at,
                     player_id,team_id,match_status,quote_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id''',
                    (capture_id, row['bookmaker'], row['market'], row['player_name'], row['line'], row['side'],
                     row['american_price'], row['source_updated_at'], player[0] if player else None,
                     player[1] if player else None, 'candidate_unverified' if player else 'unresolved',
                     row['quote_status'] if captured < kickoff else 'after_kickoff')).fetchone()[0]
                summary['player_candidates'] += bool(player)
                if player and row['quote_status'] == 'fresh' and captured < min(kickoff, game['kickoff']):
                    pending.append((qid, row, player))
        # Commit actual quotes before doing slower model inference. Never fabricate a missing line.
        game_attempts = 0
        for qid, row, player in pending:
            key = (game['game_id'], player[0], row['market'], row['line'])
            if (summary['forward_analyses'] + summary['analysis_errors'] >= analyze_limit
                or game_attempts >= per_game_limit or key in analyzed):
                continue
            analyzed.add(key)
            game_attempts += 1
            request = dict(game_id=game['game_id'], player_id=int(player[0]), team_id=player[1],
                           stat=MARKETS[row['market']], line=row['line'], as_of=captured.isoformat())
            try:
                from cfb.analyze import analyze
                analysis = analyze(request)
                analysis['market_identity_status'] = 'candidate_unverified'
                with connect() as conn:
                    # Late results are not counted as forward predictions.
                    if datetime.now(timezone.utc) >= min(kickoff, game['kickoff']):
                        continue
                    conn.execute('''INSERT INTO cfb_model_v1.forward_research(quote_id,request,analysis)
                        VALUES (%s,%s,%s)''', (qid, json.dumps(request), json.dumps(analysis, allow_nan=False)))
                summary['forward_analyses'] += 1
            except (ValueError, RuntimeError):
                summary['analysis_errors'] += 1
        summary['quotes'] += len(rows)
        summary['events'].append(dict(event_id=event['id'], home=payload['home_team'], away=payload['away_team'],
                                      quotes=len(rows), game_candidate=game['game_id'] if game else None))
        print(json.dumps(summary['events'][-1]), flush=True)
    summary['http_calls'] = client.calls
    print(json.dumps(summary, indent=2))
    return summary
