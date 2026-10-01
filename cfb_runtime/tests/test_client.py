import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cfb.data import Client, connect


class ClientTests(unittest.TestCase):
    def test_pooled_connection_sets_timeout_after_connect(self):
        url = 'postgresql://user:example@pooler.example/db'
        with patch.dict('os.environ', {'DATABASE_URL': url}), patch('cfb.data.psycopg.connect') as pg:
            conn = connect()
            pg.assert_called_once_with(url, connect_timeout=10)
            conn.execute.assert_called_once_with("SET LOCAL statement_timeout = '30s'")
            conn.commit.assert_not_called()
            conn.close.assert_not_called()

    def test_timeout_setup_failure_closes_connection(self):
        with patch.dict('os.environ', {'DATABASE_URL': 'postgresql://example/db'}), patch('cfb.data.psycopg.connect') as pg:
            pg.return_value.execute.side_effect = RuntimeError('setup failed')
            with self.assertRaisesRegex(RuntimeError, 'setup failed'):
                connect()
            pg.return_value.close.assert_called_once()

    def test_invalid_database_url_never_connects(self):
        with patch.dict('os.environ', {'DATABASE_URL': ''}), patch('cfb.data.psycopg.connect') as pg:
            with self.assertRaises(ValueError):
                connect()
            pg.assert_not_called()

    def test_zero_budget_never_sends_request(self):
        with patch.dict('os.environ', {'CFBD_API_KEY': 'test-secret'}):
            client = Client(max_calls=0)
        with patch('cfb.data.requests.get') as get:
            with self.assertRaisesRegex(RuntimeError, 'budget exhausted'):
                client.get('/games', cache=False)
            get.assert_not_called()

    def test_cache_contains_no_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict('os.environ', {'CFBD_API_KEY': 'test-secret'}), \
                 patch('cfb.data.ROOT', Path(directory)), \
                 patch('cfb.data.connect'), \
                 patch('cfb.data.requests.get') as get:
                get.return_value.status_code = 200
                get.return_value.json.return_value = [{'id': 1}]
                get.return_value.headers = {}
                client = Client()
                expected = client.get('/games', {'year': 2021})
                self.assertEqual(expected, client.get('/games', {'year': 2021}))
                self.assertEqual(get.call_count, 1)
                record = next((Path(directory) / 'data/raw').glob('*.json')).read_text()
                self.assertNotIn('test-secret', record)
                self.assertLessEqual(json.loads(record)['retrieved_at'], time.time())


if __name__ == '__main__':
    unittest.main()
