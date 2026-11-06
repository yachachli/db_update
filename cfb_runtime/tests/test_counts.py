import copy
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
from cfb.counts import probability, draws, predict, split, COUNT_MARKETS
from cfb.analyze import validate_request, load_artifacts
from cfb.markets import MARKETS
from cfb.renderer import render_analysis
from cfb.tracking import evaluate
import test_tracking


class CountTests(unittest.TestCase):
    def test_nonnegative_discrete_support_and_push(self):
        residuals = np.linspace(-10, 10, 201)
        self.assertTrue((draws(2, residuals) >= 0).all())
        zero = probability(2, 0, residuals)
        self.assertEqual(zero['under'], 0)
        self.assertGreater(zero['push'], 0)
        half = probability(2, .5, residuals)
        self.assertEqual(half['push'], 0)
        self.assertAlmostEqual(sum(half.values()), 1)
        self.assertGreaterEqual(half['over'], probability(2, 4.5, residuals)['over'])

    def test_bad_calibration_fails_without_yardage_fallback(self):
        for values in ([0]*99, [float('nan')]*100, [[0]*100]):
            with self.assertRaises(ValueError): probability(2, 1.5, values)
        with self.assertRaises(ValueError): probability(-1, 1.5, [0]*100)
        with self.assertRaises(ValueError): probability(1, -1, [0]*100)
        with self.assertRaises(ValueError): predict({'kind':'boosted_median'}, pd.DataFrame())

    def test_market_request_and_public_units(self):
        for market, stat in COUNT_MARKETS.items():
            self.assertEqual(MARKETS[market], stat)
            request = dict(game_id=1, player_id=2, stat=stat, line=4.5)
            validate_request(request)
            out = render_analysis(dict(input={**request,'team_code':'X','opponent_abv':'Y'},
                projected_value=5., projected_volume=5., projected_stat=stat,
                model_lean='over', participation_probability=.9))
            self.assertNotIn('yards', out['long_answer'])
            self.assertIn(stat, out['long_answer'])

    def test_temporal_overlap_rejected(self):
        frame = pd.DataFrame([dict(season=y, actual_volume=1, kickoff=pd.Timestamp('2025-01-01'),
                                   data_cutoff=pd.Timestamp('2025-01-01'))
                              for y in (2024,2025,2026) for _ in range(100)])
        with self.assertRaisesRegex(ValueError,'overlaps'): split(frame,2026)

    def test_count_settlement_uses_volume_not_yards(self):
        row = test_tracking.TrackingTests().row(market='player_receptions', line=4,
                                  actual_yards=80, actual_volume=4)
        row['request'].update(stat='receptions', line=4)
        result = evaluate(row)
        self.assertEqual(result['observed_side'], 'push')
        self.assertEqual(result['actual_volume'], 4)
        self.assertIsNone(result['actual_yards'])
        row['actual_volume'] = None
        self.assertEqual(evaluate(row)['status'], 'missing_box_score_outcome')
        row['actual_volume'] = 0
        self.assertEqual(evaluate(row)['observed_side'], 'under')
        row['actual_volume'] = -1
        self.assertEqual(evaluate(row)['status'], 'invalid_box_score_outcome')

    def test_artifact_loader_requires_count_calibration_hash(self):
        import tempfile,json,hashlib,joblib
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); saved=root/'models/saved/pou_v1';saved.mkdir(parents=True)
            joblib.dump({'kind':'count_trailing5'},saved/'count.joblib')
            joblib.dump({'participation':'fixture'},saved/'play.joblib')
            (saved/'cal.json').write_text('{"target":"volume"}')
            sha=lambda name:hashlib.sha256((saved/name).read_bytes()).hexdigest()
            entry=dict(target='volume',artifact='count.joblib',calibration='cal.json',
                participation_artifact='play.joblib',sha256=sha('count.joblib'),
                participation_sha256=sha('play.joblib'),calibration_sha256=sha('cal.json'))
            (saved/'manifest.json').write_text(json.dumps({'categories':{'receiving_volume':entry}}))
            with patch('cfb.analyze.ROOT',root):
                load_artifacts('receiving_volume')
                with self.assertRaisesRegex(ValueError,'Missing calibrated'):load_artifacts('passing_volume')
                (saved/'cal.json').write_text('{"target":"yards"}')
                with self.assertRaisesRegex(ValueError,'integrity'):load_artifacts('receiving_volume')

    def test_quotes_parse_all_count_markets_without_fake_lines(self):
        from cfb.markets import quotes,timestamp
        for market in COUNT_MARKETS:
            event={'bookmakers':[{'key':'book','markets':[{'key':market,'last_update':'2026-11-06T12:00:00Z',
                'outcomes':[{'name':'Over','description':'Player','point':4.5,'price':-110},
                            {'name':'Under','description':'Player','point':4.5,'price':100}]}]}]}
            result=quotes(event,timestamp('2026-11-06T12:01:00Z'))
            self.assertEqual(len(result),2)
            self.assertEqual(result[1]['american_price'],100)
            self.assertEqual(result[0]['market'],market)
        self.assertEqual(quotes({'bookmakers':[]},timestamp('2026-11-06T12:01:00Z')),[])
