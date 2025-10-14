from unittest import TestCase

from hummingbot.connector.exchange.rkex import rkex_utils as utils


class RkexUtilsTests(TestCase):

    def test_is_exchange_information_valid_for_active_symbol(self):
        """Test that TRADING status is valid"""
        exchange_info = {"status": "TRADING"}
        self.assertTrue(utils.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_for_inactive_symbol(self):
        """Test that non-TRADING status is invalid"""
        exchange_info = {"status": "BREAK"}
        self.assertFalse(utils.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_with_missing_status(self):
        """Test that missing status defaults to TRADING (valid)"""
        exchange_info = {}
        self.assertTrue(utils.is_exchange_information_valid(exchange_info))

    def test_is_exchange_information_valid_for_trading_symbol(self):
        """Test full exchange info structure"""
        exchange_info = {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT"
        }
        self.assertTrue(utils.is_exchange_information_valid(exchange_info))
