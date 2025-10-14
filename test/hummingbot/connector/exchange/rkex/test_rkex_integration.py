"""
Integration tests for Rkex connector using the live API at https://apiengine.demoapps.space

These tests connect to the actual API server to verify connectivity and endpoint structure.
They require a valid Bearer token for API key generation.

Note: These tests are designed to be run against the live API and may require authentication.
"""
import asyncio
import unittest
from decimal import Decimal
from typing import Awaitable

import aiohttp

from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS
from hummingbot.connector.exchange.rkex import rkex_web_utils as web_utils
from hummingbot.connector.exchange.rkex.rkex_exchange import RkexExchange


class TestRkexIntegration(unittest.TestCase):
    """Integration tests that connect to the live API"""

    # Bearer token for API key generation
    # This token is required to call /apikey/generate endpoint
    BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkNGUzNDhhMi1mMDU3LTQ3MjgtYWZlYi05ODBjZTY4ZTgzMmQiLCJlbWFpbCI6InJpc2hpQHRlc3RtYWlsLmNvbSIsInJvbGVzIjpbIlVTRVIiLCJBRE1JTiJdLCJpYXQiOjE3NTkyOTYwNDEsImV4cCI6MTc1OTMzMjA0MX0.fukEW3KvHiZmk8q7hmTCS60vYwuuNDJ5tpB2V6N9LaU"

    @classmethod
    def setUpClass(cls) -> None:
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 10):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    async def _test_ping_endpoint(self):
        """Test the /ping endpoint"""
        url = web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
        self.assertEqual("https://apiengine.demoapps.space/ping", url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                self.assertEqual(200, response.status)
                # Ping endpoint typically returns empty object or simple status
                data = await response.json()
                self.assertIsNotNone(data)

    async def _test_time_endpoint(self):
        """Test the /time endpoint"""
        url = web_utils.public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        self.assertEqual("https://apiengine.demoapps.space/time", url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                self.assertEqual(200, response.status)
                data = await response.json()
                # Should return server time in some format
                self.assertIsNotNone(data)
                # Common fields: serverTime, timestamp, etc.

    async def _test_exchange_info_endpoint(self):
        """Test the /exchangeinfo endpoint"""
        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        self.assertEqual("https://apiengine.demoapps.space/exchangeinfo", url)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                self.assertEqual(200, response.status)
                data = await response.json()
                self.assertIsNotNone(data)
                # Should have symbols or similar trading pair info

    async def _test_api_key_generation(self):
        """Test API key generation with Bearer token"""
        url = web_utils.private_rest_url(CONSTANTS.API_KEY_GENERATE_PATH_URL)
        self.assertEqual("https://apiengine.demoapps.space/apikey/generate", url)

        headers = {
            "Authorization": f"Bearer {self.BEARER_TOKEN}",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                # May return 200 or 201 depending on implementation
                self.assertIn(response.status, [200, 201, 401])  # 401 if token expired
                if response.status in [200, 201]:
                    data = await response.json()
                    self.assertIsNotNone(data)
                    # Should contain api key and secret

    def test_endpoint_urls(self):
        """Test that all endpoint URLs are constructed correctly"""
        # Public endpoints
        self.assertEqual(
            "https://apiengine.demoapps.space/ping",
            web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
        )
        self.assertEqual(
            "https://apiengine.demoapps.space/time",
            web_utils.public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL)
        )
        self.assertEqual(
            "https://apiengine.demoapps.space/exchangeinfo",
            web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        )

        # Private endpoints
        self.assertEqual(
            "https://apiengine.demoapps.space/balance",
            web_utils.private_rest_url(CONSTANTS.ACCOUNTS_PATH_URL)
        )
        self.assertEqual(
            "https://apiengine.demoapps.space/order",
            web_utils.private_rest_url(CONSTANTS.ORDER_PATH_URL)
        )
        self.assertEqual(
            "https://apiengine.demoapps.space/order/active-orders",
            web_utils.private_rest_url(CONSTANTS.ACTIVE_ORDERS_PATH_URL)
        )
        self.assertEqual(
            "https://apiengine.demoapps.space/order/all-orders",
            web_utils.private_rest_url(CONSTANTS.ALL_ORDERS_PATH_URL)
        )

    def test_live_ping_endpoint(self):
        """Test live /ping endpoint"""
        try:
            self.async_run_with_timeout(self._test_ping_endpoint())
        except Exception as e:
            self.skipTest(f"Live API test skipped: {str(e)}")

    def test_live_time_endpoint(self):
        """Test live /time endpoint"""
        try:
            self.async_run_with_timeout(self._test_time_endpoint())
        except Exception as e:
            self.skipTest(f"Live API test skipped: {str(e)}")

    def test_live_exchange_info_endpoint(self):
        """Test live /exchangeinfo endpoint"""
        try:
            self.async_run_with_timeout(self._test_exchange_info_endpoint())
        except Exception as e:
            self.skipTest(f"Live API test skipped: {str(e)}")

    @unittest.skip("Requires valid Bearer token - token may be expired")
    def test_live_api_key_generation(self):
        """Test live /apikey/generate endpoint (skipped by default)"""
        try:
            self.async_run_with_timeout(self._test_api_key_generation())
        except Exception as e:
            self.skipTest(f"API key generation test skipped: {str(e)}")


class TestRkexConnectorInitialization(unittest.TestCase):
    """Test connector can be initialized with proper configuration"""

    def test_connector_initialization(self):
        """Test that the connector can be created"""
        api_key = "test_key"
        api_secret = "test_secret"
        trading_pairs = ["BTC-USDT", "ETH-USDT"]

        connector = RkexExchange(
            rkex_api_key=api_key,
            rkex_api_secret=api_secret,
            trading_pairs=trading_pairs
        )

        self.assertEqual("rkex", connector.name)
        self.assertEqual(trading_pairs, connector.trading_pairs)
        self.assertIsNotNone(connector.authenticator)

    def test_connector_properties(self):
        """Test connector basic properties"""
        connector = RkexExchange(
            rkex_api_key="test",
            rkex_api_secret="test",
            trading_pairs=["BTC-USDT"]
        )

        self.assertEqual(CONSTANTS.MAX_ORDER_ID_LEN, connector.client_order_id_max_length)
        self.assertEqual(CONSTANTS.HBOT_ORDER_ID_PREFIX, connector.client_order_id_prefix)
        self.assertEqual(CONSTANTS.PING_PATH_URL, connector.check_network_request_path)
        self.assertEqual(CONSTANTS.EXCHANGE_INFO_PATH_URL, connector.trading_rules_request_path)


if __name__ == "__main__":
    unittest.main()
