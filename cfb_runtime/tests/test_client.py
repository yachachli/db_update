import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from cfb.data import Client


class ClientTests(unittest.TestCase):
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
