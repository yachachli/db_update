"""User-selected linear transition to this season after 20 completed games."""
TRANSITION_GAMES = 20
TRANSITION_POLICY = 'linear-20-games-v1'


def current_season_weight(games, transition_games=TRANSITION_GAMES):
    if transition_games <= 0 or games < 0:
        raise ValueError('Games must be nonnegative and transition length positive')
    return min(float(games) / transition_games, 1.0)


def prior_season_weight(games, transition_games=TRANSITION_GAMES):
    return 1.0 - current_season_weight(games, transition_games)
