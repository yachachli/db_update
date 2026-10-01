import unittest
from copy import deepcopy
from cfb.markets import event_candidate, player_candidate, quotes, timestamp, collect


class MarketTests(unittest.TestCase):
    def test_event_match_requires_teams_time_and_unique_candidate(self):
        event = dict(home_team='Ohio State Buckeyes', away_team='Michigan Wolverines', commence_time='2026-10-03T16:00:00Z')
        game = dict(home_team='Ohio State', away_team='Michigan', kickoff=timestamp(event['commence_time']))
        self.assertEqual(event_candidate(event, [game]), game)
        self.assertIsNone(event_candidate(event, [game, game]))
        self.assertIsNone(event_candidate({**event, 'home_team': 'Ohio Bobcats'}, [game]))
        self.assertIsNone(event_candidate({**event, 'commence_time': '2026-10-04T16:00:00Z'}, [game]))

    def test_players_ambiguous_suffixes_and_punctuation(self):
        p = dict(player_id='1', team_id=2, player_name="Jai'Den Thomas")
        self.assertEqual(player_candidate('Jaiden Thomas', [p]), ('1', 2))
        self.assertIsNone(player_candidate('Jaiden Thomas Jr', [p]))
        self.assertIsNone(player_candidate('Jaiden Thomas', [p, {**p, 'player_id': '3'}]))
        self.assertIsNone(player_candidate('Jaiden Thomas', [p, {**p, 'team_id': 4}]))

    def test_quotes_keep_prices_and_block_stale_nonfinite_alternates(self):
        event = {'bookmakers': [{'key': 'test', 'markets': [{'key': 'player_rush_yds',
            'last_update': '2026-10-01T12:00:00Z', 'outcomes': [
                {'name': 'Over', 'description': 'Player One', 'point': 49.5, 'price': -110},
                {'name': 'Under', 'description': 'Player One', 'point': 49.5, 'price': 100}]}]}]}
        rows = quotes(event, timestamp('2026-10-01T12:01:00Z'))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['american_price'], -110)
        self.assertEqual(rows[0]['quote_status'], 'fresh')
        self.assertEqual(quotes(event, timestamp('2026-10-01T13:00:00Z'))[0]['quote_status'], 'stale_or_future')
        self.assertEqual(quotes(event, timestamp('2026-10-01T11:00:00Z'))[0]['quote_status'], 'stale_or_future')
        broken = deepcopy(event)
        broken['bookmakers'][0]['markets'][0]['outcomes'][0]['point'] = float('nan')
        self.assertEqual(len(quotes(broken, timestamp('2026-10-01T12:01:00Z'))), 1)
        broken['bookmakers'][0]['markets'][0]['key'] += '_alternate'
        self.assertEqual(quotes(broken, timestamp('2026-10-01T12:01:00Z')), [])

    def test_invalid_budgets_rejected_before_io(self):
        with self.assertRaises(ValueError):
            collect(1, 1, per_game_limit=0)
        for limit, analyses in [(0, 1), (11, 1), (1, 11), (1, -1)]:
            with self.assertRaises(ValueError):
                collect(limit, analyses)
