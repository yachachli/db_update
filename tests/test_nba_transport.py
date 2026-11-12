import unittest
from unittest.mock import AsyncMock, patch
import httpx
from db_update.runners import nba


class ProviderRetries(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_retries_then_returns_success(self):
        request = httpx.Request('GET', 'https://example.test/box')
        response = httpx.Response(200, request=request, json={'statusCode': 200, 'body': []})
        client = AsyncMock()
        client.headers = {}
        client.get.side_effect = [httpx.ReadTimeout('timeout', request=request), response]
        with patch.object(nba.asyncio, 'sleep', new_callable=AsyncMock):
            self.assertEqual(await nba._fetch_json(client, str(request.url)), response.json())
        self.assertEqual(client.get.await_count, 2)

    async def test_exhausted_transport_retries_fail_visibly(self):
        client = AsyncMock()
        client.headers = {}
        client.get.side_effect = httpx.ConnectError('unavailable')
        with patch.object(nba.asyncio, 'sleep', new_callable=AsyncMock):
            with self.assertRaises(httpx.ConnectError):
                await nba._fetch_json(client, 'https://example.test/box')
        self.assertEqual(client.get.await_count, 6)
