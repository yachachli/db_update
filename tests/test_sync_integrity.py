import importlib.util
from pathlib import Path
import unittest
from unittest.mock import AsyncMock,patch,MagicMock
spec=importlib.util.spec_from_file_location('nba_sync',Path(__file__).parents[1]/'scripts/sync_daily_nba_games.py')
sync=importlib.util.module_from_spec(spec);spec.loader.exec_module(sync)

class SyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_wrong_date_cannot_write(self):
        conn=MagicMock()
        with patch.object(sync,'fetch_predictions',AsyncMock(return_value={'date':'20261025','predictions':[]})):
            with self.assertRaisesRegex(RuntimeError,'wrong date'):
                await sync.sync_predictions_for_date(conn,'url',None,None,'20261024')
        conn.transaction.assert_not_called()

    async def test_stale_generator_cannot_publish(self):
        conn=MagicMock()
        payload={'date':'20261024','predictions':[{'home_team':'BOS','away_team':'NYK','model_context':{'season':'2025-26'}}]}
        with patch.object(sync,'fetch_predictions',AsyncMock(return_value=payload)):
            with self.assertRaisesRegex(RuntimeError,'provenance'):
                await sync.sync_predictions_for_date(conn,'url',None,None,'20261024')
        conn.transaction.assert_not_called()

    async def test_partial_failure_fails_job(self):
        conn=AsyncMock()
        with patch.dict('os.environ',{'NBA_GAME_PREDICTOR_URL':'url'}),patch.object(sync,'get_connection',AsyncMock(return_value=conn)),patch.object(sync,'sync_predictions_for_date',AsyncMock(side_effect=[RuntimeError('missing data'),None,None])) as generate:
            with self.assertRaisesRegex(RuntimeError,'publication failed'):
                await sync.main()
        self.assertEqual(generate.await_count,3)
        conn.close.assert_awaited_once()
