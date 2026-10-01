import unittest
from cfb.markets import timestamp
from cfb.tracking import evaluate, summarize


class TrackingTests(unittest.TestCase):
    def row(self, **updates):
        row = dict(created_at=timestamp('2026-10-01T12:01:00Z'),
                   captured_at=timestamp('2026-10-01T12:00:00Z'),
                   kickoff=timestamp('2026-10-02T00:00:00Z'),
                   provider_kickoff=timestamp('2026-10-02T00:00:00Z'),
                   request=dict(game_id=1, player_id=2, team_id=3, line=49.5,
                                stat='rush yards', as_of='2026-10-01T12:00:00Z'),
                   game_id=1, player_id='2', team_id=3, market='player_rush_yds', line=49.5,
                   completed=True, actual_team_id=3, actual_yards=40,
                   analysis=dict(projected_value=45., conditional_probabilities=dict(over=.4, under=.6, push=0)))
        row.update(updates)
        return row

    def test_pending_never_uses_partial_stats(self):
        result = evaluate(self.row(completed=False))
        self.assertEqual(result['status'], 'pending_game')
        self.assertIsNone(result['actual_yards'])

    def test_absent_not_zero_but_explicit_zero_is_observed(self):
        self.assertEqual(evaluate(self.row(actual_yards=None, actual_team_id=None))['status'], 'missing_box_score_outcome')
        self.assertEqual(evaluate(self.row(actual_yards=0))['actual_yards'], 0)
        self.assertEqual(evaluate(self.row(actual_yards=-5))['actual_yards'], -5)

    def test_diagnostics_not_settlement(self):
        result = evaluate(self.row())
        self.assertEqual(result['absolute_error'], 5)
        self.assertAlmostEqual(result['conditional_brier'], .32)
        self.assertEqual(result['observed_side'], 'under')
        self.assertFalse(result['identity_verified'])
        self.assertFalse(result['market_ready'])
        self.assertEqual(result['sportsbook_settlement'], 'unverified')

    def test_push_three_class(self):
        row = self.row(line=40)
        row['request']['line'] = 40
        self.assertEqual(evaluate(row)['observed_side'], 'push')
        self.assertAlmostEqual(evaluate(row)['conditional_brier'], 1.52)

    def test_mismatched_team_or_late_prediction_blocked(self):
        self.assertEqual(evaluate(self.row(actual_team_id=4))['status'], 'outcome_team_mismatch')
        self.assertEqual(evaluate(self.row(created_at=timestamp('2026-10-02T00:00:00Z')))['status'], 'invalid_pregame_timing')
        row = self.row()
        row['request']['player_id'] = 99
        self.assertEqual(evaluate(row)['status'], 'identity_or_request_mismatch')

    def test_abstention_and_bad_probability_excluded(self):
        result = evaluate(self.row(analysis={'projected_value': None}))
        self.assertEqual(result['status'], 'observed_without_projection')
        self.assertIsNone(result['absolute_error'])
        row = self.row()
        row['analysis']['conditional_probabilities']['over'] = float('nan')
        self.assertIsNone(evaluate(row)['conditional_brier'])
        self.assertEqual(summarize([result])['candidate_identity_projection_count'], 0)
        self.assertIsNone(summarize([])['candidate_identity_mae'])
