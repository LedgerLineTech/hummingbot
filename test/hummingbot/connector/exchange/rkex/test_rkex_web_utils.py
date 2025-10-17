import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import hummingbot.connector.exchange.rkex.rkex_constants as CONSTANTS
import hummingbot.connector.exchange.rkex.rkex_web_utils as web_utils


class RkexWebUtilsTests(TestCase):

    def test_public_rest_url(self):
        """Test public REST URL construction"""
        path_url = "ping"
        domain = CONSTANTS.DEFAULT_DOMAIN

        expected_url = f"{CONSTANTS.REST_URL}{CONSTANTS.PUBLIC_API_VERSION}{path_url}"
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))
        self.assertEqual("https://apiengine.demoapps.space/ping", expected_url)

    def test_private_rest_url(self):
        """Test private REST URL construction"""
        path_url = "balance"
        domain = CONSTANTS.DEFAULT_DOMAIN

        expected_url = f"{CONSTANTS.REST_URL}{CONSTANTS.PRIVATE_API_VERSION}{path_url}"
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))
        self.assertEqual("https://apiengine.demoapps.space/balance", expected_url)

    def test_public_rest_url_with_different_endpoints(self):
        """Test various public endpoint URLs"""
        test_cases = [
            ("ping", "https://apiengine.demoapps.space/ping"),
            ("time", "https://apiengine.demoapps.space/time"),
            ("exchangeinfo", "https://apiengine.demoapps.space/exchangeinfo"),
            ("depth", "https://apiengine.demoapps.space/depth"),
        ]

        for path, expected_url in test_cases:
            url = web_utils.public_rest_url(path)
            self.assertEqual(expected_url, url)

    def test_private_rest_url_with_different_endpoints(self):
        """Test various private endpoint URLs"""
        test_cases = [
            ("order", "https://apiengine.demoapps.space/order"),
            ("order/all-orders", "https://apiengine.demoapps.space/order/all-orders"),
            ("order/active-orders", "https://apiengine.demoapps.space/order/active-orders"),
            ("balance", "https://apiengine.demoapps.space/balance"),
            ("myTrades", "https://apiengine.demoapps.space/myTrades"),
        ]

        for path, expected_url in test_cases:
            url = web_utils.private_rest_url(path)
            self.assertEqual(expected_url, url)

    @patch("hummingbot.connector.exchange.rkex.rkex_web_utils.build_api_factory_without_time_synchronizer_pre_processor")
    def test_get_current_server_time(self, mock_build_factory):
        """Test server time retrieval and conversion from ISO format"""
        async def run_test():
            # Mock the API response with ISO date format
            mock_response = {"ServerDate": "2025-10-17T19:50:34.277Z"}

            mock_rest_assistant = AsyncMock()
            mock_rest_assistant.execute_request = AsyncMock(return_value=mock_response)

            mock_api_factory = MagicMock()
            mock_api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)
            mock_build_factory.return_value = mock_api_factory

            # Call the function
            server_time = await web_utils.get_current_server_time()

            # Verify it returns a timestamp in milliseconds
            self.assertIsInstance(server_time, float)
            self.assertGreater(server_time, 0)

            # Expected timestamp for "2025-10-17T19:50:34.277Z" is approximately 1760730634277
            # Allow some tolerance for conversion precision
            expected_time = 1760730634277.0
            self.assertAlmostEqual(server_time, expected_time, delta=1000)

        asyncio.get_event_loop().run_until_complete(run_test())
