from unittest import TestCase

import hummingbot.connector.exchange.apiengine.apiengine_constants as CONSTANTS
import hummingbot.connector.exchange.apiengine.apiengine_web_utils as web_utils


class ApiEngineWebUtilsTests(TestCase):

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
