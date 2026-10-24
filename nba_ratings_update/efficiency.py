"""Descriptive box-score efficiencies, separate from the calibrated margin model."""
from collections import defaultdict
from datetime import date, timedelta
import math
from .transition_policy import current_season_weight, TRANSITION_POLICY


def measured_efficiencies(games, rows, cutoff):
    """Use only complete, score-reconciled team boxes and shared possessions."""
    boxes = {(r['game_id'], r['team']): r for r in rows}
    totals = defaultdict(lambda: [0., 0., 0., 0])
    for g in games:
        pair = [boxes.get((g['game_id'], g[s+'_team'])) for s in ('home', 'away')]
        if any(r is None for r in pair):
            continue
        if any(r['players'] != r['unique_players'] or r['minutes'] < 225
               or abs(r['points'] - float(g[s+'_score'])) > 1
               or r['missing'] for s, r in zip(('home', 'away'), pair)):
            continue
        possessions = sum(r['fga'] + .44*r['fta'] - r['orb'] + r['tov'] for r in pair)/2
        if not math.isfinite(possessions) or not 60 <= possessions <= 180:
            continue
        age = (date.fromisoformat(cutoff)-date.fromisoformat(str(g['game_date'])[:10])).days
        if age <= 0:
            continue
        w = max(.1, .5**(age/30))
        for side, other in [('home','away'), ('away','home')]:
            t = totals[g[side+'_team']]
            t[0] += 100*float(g[side+'_score'])/possessions*w
            t[1] += 100*float(g[other+'_score'])/possessions*w
            t[2] += w
            t[3] += 1
    return {team: {'adj_off': t[0]/t[2], 'adj_def': t[1]/t[2], 'games': t[3]}
            for team,t in totals.items()}


def load_efficiencies(prior_games, current_games, cutoff, connection_factory):
    conn = connection_factory()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT game_id,
                CASE team_abv WHEN 'GS' THEN 'GSW' WHEN 'NO' THEN 'NOP'
                WHEN 'NY' THEN 'NYK' WHEN 'PHO' THEN 'PHX' WHEN 'SA' THEN 'SAS'
                ELSE team_abv END AS team,
                COUNT(*) AS players, COUNT(DISTINCT player_id) AS unique_players,
                SUM(minutes_played) AS minutes, SUM(points) AS points,
                SUM(field_goal_attempts) AS fga, SUM(free_throw_attempts) AS fta,
                SUM(offensive_rebounds) AS orb, SUM(turnovers) AS tov,
                COUNT(*) FILTER (WHERE field_goal_attempts IS NULL OR free_throw_attempts IS NULL
                  OR offensive_rebounds IS NULL OR turnovers IS NULL OR points IS NULL
                  OR minutes_played IS NULL) AS missing
                FROM nba_player_game_stats WHERE game_date >= %s AND game_date < %s
                GROUP BY game_id, team_abv""",
                (min(str(g['game_date'])[:10] for g in prior_games), cutoff))
            names = [c[0] for c in cur.description]
            rows = [dict(zip(names,r)) for r in cur.fetchall()]
    finally:
        conn.close()
    for row in rows:
        for key in ('minutes','points','fga','fta','orb','tov'):
            row[key] = float(row[key] or 0)
    anchor = (max(date.fromisoformat(str(g['game_date'])[:10]) for g in prior_games)
              + timedelta(days=1)).isoformat()
    prior = measured_efficiencies(prior_games, rows, anchor)
    current = measured_efficiencies(current_games, rows, cutoff)
    expected = defaultdict(int)
    for game in current_games:
        expected[game['home_team']] += 1
        expected[game['away_team']] += 1
    for team, count in expected.items():
        if current.get(team, {}).get('games', 0) != count:
            raise RuntimeError(f'Incomplete current-season box scores for {team}; ingest before analysis')
    output = {}
    for team in {g[s+'_team'] for g in prior_games for s in ('home','away')}:
        old, new = prior.get(team), current.get(team)
        n = new['games'] if new else 0
        w = current_season_weight(n)
        if w == 1.0:
            output[team] = {key: new[key] for key in ('adj_off', 'adj_def')}
            output[team].update(status='measured', prior_games=0, current_games=n,
                                current_weight=w, method='boxscore_possessions_recency_blend',
                                transition_policy=TRANSITION_POLICY)
            continue
        # Insufficient boxes must not become an invented league-average rating.
        if not old or old['games'] < 20:
            output[team] = {'adj_off': None, 'adj_def': None, 'status': 'insufficient_boxscores'}
            continue
        output[team] = {key: (1-w)*old[key]+w*new[key] if new else old[key]
                        for key in ('adj_off','adj_def')}
        output[team].update(status='measured', prior_games=old['games'], current_games=n,
                            current_weight=w, method='boxscore_possessions_recency_blend',
                            transition_policy=TRANSITION_POLICY)
    return output
