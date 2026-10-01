from datetime import datetime, timezone
import unittest
from unittest.mock import patch
from cfb.partitions import recent
from cfb.pipeline import plan, run, season_for


class PipelineTests(unittest.TestCase):
    def test_season_rollover(self):
        self.assertEqual(season_for(datetime(2027, 1, 15)), 2026)
        self.assertEqual(season_for(datetime(2027, 8, 15)), 2027)

    def test_postseason_partition_order(self):
        rows = [(1, 15, 'regular', datetime(2026,12,1)),
                (2, 1, 'postseason', datetime(2027,1,1)),
                (3, 1, 'postseason', datetime(2027,1,2))]
        self.assertEqual(recent(rows, 1), rows[1:])
        self.assertEqual(recent(rows, None), rows)
        with self.assertRaises(ValueError): recent(rows, 7)

    def test_dry_run_never_connects(self):
        with patch('cfb.pipeline.connect') as connect:
            result = run(dry_run=True, season=2026)
            connect.assert_not_called()
        self.assertFalse(result['retrain'])
        self.assertEqual(result['stages'][-1], 'outcomes')

    def test_market_plan_no_auto_training(self):
        result = plan('markets', 2026)
        self.assertEqual(result['analyses'], 0)
        self.assertNotIn('train', result['stages'])
        for args in [('wrong',2026,3,0), ('weekly',2026,0,0), ('markets',2026,3,11)]:
            with self.assertRaises(ValueError): plan(*args)

    def test_lock_blocks_overlap(self):
        with patch.dict('os.environ', {'DATABASE_URL':'test', 'CFBD_API_KEY':'test'}), patch('cfb.pipeline.connect') as connect:
            connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (False,)
            with self.assertRaisesRegex(RuntimeError, 'already running'):
                run(season=2026)
