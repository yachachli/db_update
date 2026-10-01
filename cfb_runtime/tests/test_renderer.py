import copy
import json
import unittest
from unittest.mock import patch
from cfb.analyze import analyze
from cfb.renderer import render_analysis


class RendererTests(unittest.TestCase):
    def fixture(self):
        return {
            'input': {'game_id': 1, 'as_of': '2026-09-30T00:00:00Z', 'player_id': 2,
                      'team_code': 'A', 'opponent_abv': 'B', 'line': 49.5, 'stat': 'rush yards'},
            'player_name': 'Example Player', 'opponent': 'B', 'projected_value': 46.3,
            'projected_stat': 'rush yards', 'projected_volume': 14., 'model_lean': 'under',
            'grade': 0, 'injury': None, 'graphs': [], 'player_position': 'Unknown',
            'history': {'games': 3, 'over_hits': 1, 'under_hits': 2, 'pushes': 0},
            'market_ready': False, 'recommendation_status': 'abstain',
            'abstention_reasons': ['availability_unconfirmed'],
            'scope': 'Conditional research analysis, not a sportsbook recommendation',
            'insights': ['No bet recommendation'], 'short_answer': 'Research-only',
            'long_answer': 'Market grading remains unverified',
            'conditional_probabilities': {'under': .54, 'over': .46, 'push': 0},
        }

    def test_exact_nfl_public_fields_without_diagnostics(self):
        original = self.fixture()
        saved = copy.deepcopy(original)
        result = render_analysis(original)
        self.assertEqual(set(result), {
            'over_under', 'grade', 'league', 'injury', 'insights', 'input',
            'short_answer', 'long_answer', 'player_position', 'graphs',
            'projected_stat', 'projected_value', 'pre_injury_projected_value',
            'injury_adjustment_notes', 'version'})
        self.assertEqual(set(result['input']), {'player_id', 'team_code', 'line', 'stat',
                                               'opponent_abv', 'player_name', 'player_pic'})
        self.assertEqual(result['over_under'], 'under')
        self.assertEqual(result['projected_value'], 46.3)
        self.assertEqual(result['grade'], 0)
        self.assertEqual(original, saved)
        text = json.dumps(result).lower()
        for phrase in ('research-only', 'no bet recommendation', 'unverified', 'market_ready',
                       'abstention', 'conditional_probabilities'):
            self.assertNotIn(phrase, text)
        self.assertIsNone(result['injury'])

    def test_missing_projection_remains_missing(self):
        source = self.fixture()
        source['projected_value'] = None
        result = render_analysis(source)
        self.assertIsNone(result['over_under'])
        self.assertIsNone(result['projected_value'])
        self.assertIn('Insufficient', result['long_answer'])

    def test_nonfinite_projection_is_not_serialized(self):
        source = self.fixture()
        source['projected_value'] = float('nan')
        json.dumps(render_analysis(source), allow_nan=False)

    def test_public_entrypoint_formats_internal_result(self):
        with patch('cfb.analyze.analyze_internal', return_value=self.fixture()) as internal:
            result = analyze({'request': 'example'})
        internal.assert_called_once_with({'request': 'example'})
        self.assertNotIn('market_ready', result)
        self.assertEqual(result['over_under'], 'under')
