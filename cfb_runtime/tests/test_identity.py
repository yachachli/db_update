import unittest
from cfb.identity import audit, run
from cfb.markets import timestamp


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.game = dict(home_id=1, away_id=2, home_team='Alpha', away_team='Beta',
                         kickoff=timestamp('2026-10-02T00:00:00Z'),
                         provider_kickoff=timestamp('2026-10-02T00:00:00Z'),
                         payload={'home_team': 'Alpha Bears', 'away_team': 'Beta Cats'})
        self.teams = [dict(id=1, school='Alpha', mascot='Bears'), dict(id=2, school='Beta', mascot='Cats')]
        self.player = dict(id='9', firstName='John', lastName='Smith Jr.', team_id=1, position='RB')
        self.quote = dict(player_name='John Smith Jr', player_id='9', team_id=1)

    def test_corrob_not_availability(self):
        r = audit(self.quote, self.game, self.teams, [self.player])
        self.assertEqual(r['status'], 'roster_corroborated')
        self.assertEqual(r['position'], 'RB')
        self.assertFalse(r['sportsbook_identity_verified'])
        self.assertEqual(r['availability_status'], 'unknown')

    def test_suffix_and_duplicates_not_guessed(self):
        self.assertEqual(audit({**self.quote, 'player_name': 'John Smith'}, self.game, self.teams, [self.player])['status'], 'roster_name_unresolved')
        self.assertEqual(audit(self.quote, self.game, self.teams, [self.player, {**self.player, 'id': '10'}])['status'], 'roster_name_ambiguous')

    def test_conflict_and_new_candidate(self):
        self.assertEqual(audit({**self.quote, 'player_id': '8'}, self.game, self.teams, [self.player])['status'], 'candidate_conflicts_with_roster')
        self.assertEqual(audit({**self.quote, 'player_id': None}, self.game, self.teams, [self.player])['status'], 'new_roster_candidate')

    def test_wrong_mascot_or_time_blocks(self):
        self.assertEqual(audit(self.quote, self.game, [{**self.teams[0], 'mascot': 'Dogs'}, self.teams[1]], [self.player])['status'], 'event_unresolved')
        self.game['provider_kickoff'] = timestamp('2026-10-03T00:00:00Z')
        self.assertEqual(audit(self.quote, self.game, self.teams, [self.player])['status'], 'event_unresolved')

    def test_bounds(self):
        with self.assertRaises(ValueError):
            run(11)
