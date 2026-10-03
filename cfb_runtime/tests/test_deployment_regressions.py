import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd

from cfb.analyze import analyze_internal
from cfb.artifacts import write_model_bundle
from cfb.backtest import LOOKBACK_SEASONS
from cfb.pipeline import run


class DeploymentTests(unittest.TestCase):
    def test_five_artifacts_pass_preflight_for_all_categories(self):
        artifacts = ({'analysis_season': 2026}, {}, object(), {}, object())
        with patch.dict('os.environ', {'DATABASE_URL':'test','CFBD_API_KEY':'test','ODDS_API_KEY':'test'}), \
                patch('cfb.analyze.load_artifacts', return_value=artifacts) as load, \
                patch('cfb.pipeline.connect') as connect:
            connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (False,)
            with self.assertRaisesRegex(RuntimeError, 'already running'):
                run('markets', 2026, analyses=1)
            self.assertEqual([c.args[0] for c in load.call_args_list], ['passing','rushing','receiving'])

    def test_serving_fetches_full_training_lookback(self):
        class StopAfterHistoryQuery(Exception): pass
        target = (1,2026,pd.Timestamp('2026-10-10',tz='UTC'),10,20,False,'A','B','fbs','fbs')
        artifacts = ({'analysis_season':2026}, {}, object(), {'last_calibration_kickoff':'2025-12-01T00:00:00Z'}, object())
        with patch('cfb.analyze.load_artifacts', return_value=artifacts), patch('cfb.analyze.connect') as connect:
            conn = connect.return_value.__enter__.return_value
            first = conn.execute.return_value
            first.fetchone.return_value = target
            conn.execute.side_effect = [first, StopAfterHistoryQuery()]
            with self.assertRaises(StopAfterHistoryQuery):
                analyze_internal(dict(game_id=1,player_id=2,stat='rush yards',line=40.5,as_of='2026-10-01T00:00:00Z'))
            self.assertEqual(conn.execute.call_args.args[1][1:], (2026-LOOKBACK_SEASONS,2026))

    def build_fixture(self, root):
        manifest = {'version':'cfb-pou-v2','categories':{}}
        for category in ('passing','rushing','receiving'):
            model, participation, cal = (f'{category}.joblib', f'{category}_participation.joblib', f'{category}.json')
            (root/model).write_bytes(b'yardage')
            (root/participation).write_bytes(b'participation')
            (root/cal).write_text('{}')
            manifest['categories'][category] = dict(artifact=model, calibration=cal,
                participation_artifact=participation, sha256=hashlib.sha256(b'yardage').hexdigest(),
                participation_sha256=hashlib.sha256(b'participation').hexdigest())
        (root/'manifest.json').write_text(json.dumps(manifest))
        return manifest

    def test_complete_bundle_and_checksum(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.build_fixture(root)
            archive = write_model_bundle(root, root/'bundle.zip')
            with zipfile.ZipFile(archive) as bundle:
                self.assertEqual(len(bundle.namelist()), 10)
                self.assertEqual(sum(n.endswith('_participation.joblib') for n in bundle.namelist()), 3)
            self.assertEqual((root/'bundle.zip.sha256').read_text().split()[0], hashlib.sha256(archive.read_bytes()).hexdigest())

    def test_participation_checksum_checked_before_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); self.build_fixture(root)
            (root/'passing_participation.joblib').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'integrity'):
                write_model_bundle(root, root/'bundle.zip')
            self.assertFalse((root/'bundle.zip').exists())

    def test_missing_participation_file_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); manifest = self.build_fixture(root)
            manifest['categories']['passing']['participation_artifact'] = 'missing.joblib'
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                write_model_bundle(root, root/'bundle.zip')

    def test_bundle_rejects_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); manifest = self.build_fixture(root)
            manifest['categories']['passing']['participation_artifact'] = '../outside.joblib'
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'Unsafe'):
                write_model_bundle(root, root/'bundle.zip')
