"""Public NFL-style contract; diagnostic evidence belongs in internal tracking."""
import math


def render_analysis(result):
    source = result['input']
    line = float(source['line'])
    stat = result.get('projected_stat') or source['stat']
    point = result.get('projected_value')
    supported = isinstance(point, (int, float)) and not isinstance(point, bool) and math.isfinite(point)
    side = result.get('model_lean') if supported else None
    if side not in ('over', 'under'):
        side = None
    name = result.get('player_name') or source.get('player_name') or 'Player'
    opponent = result.get('opponent') or source.get('opponent_abv', 'Unknown')
    public_input = {k: source[k] for k in ('player_id', 'team_code', 'line', 'stat', 'opponent_abv')}
    public_input['player_name'] = result.get('player_name') or source.get('player_name')
    public_input['player_pic'] = source.get('player_pic')
    if supported:
        short = f"{name}: {side.upper() if side else 'EVEN'} {line:g} {stat} against {opponent}. Projection: {point:.1f}."
        insights = [f"Projects {point:.1f} {stat} against a {line:g} line."]
        history = result.get('history', {})
        n = history.get('games', 0)
        if n:
            insights.append(f"Last {n} eligible games: {history.get('over_hits', 0)} over, "
                            f"{history.get('under_hits', 0)} under, {history.get('pushes', 0)} pushes at this line.")
        volume = result.get('projected_volume')
        if isinstance(volume, (int, float)) and math.isfinite(volume):
            unit = {'pass yards': 'pass attempts', 'rush yards': 'carries', 'rec yards': 'receptions'}[stat]
            insights.append(f"Estimated volume: {volume:.1f} {unit}; includes opponent-adjusted game context.")
    else:
        short = f"No projection available for {name}."
        insights = ['Insufficient eligible player history for this matchup.']
    return {
        'over_under': side, 'grade': result.get('grade', 0), 'league': 'CFB',
        'injury': result.get('injury'), 'insights': insights, 'input': public_input,
        'short_answer': short, 'long_answer': ' '.join(insights),
        'player_position': result.get('player_position', 'Unknown'),
        'graphs': result.get('graphs', []), 'projected_stat': stat,
        'projected_value': point if supported else None,
        'pre_injury_projected_value': None, 'injury_adjustment_notes': '', 'version': '1.0.0',
    }
