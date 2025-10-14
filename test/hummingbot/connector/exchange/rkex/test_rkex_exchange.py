import asyncio
import json
import re
import unittest
from decimal import Decimal
from typing import Awaitable, Dict, NamedTuple, Optional
from unittest.mock import patch

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS, rkex_web_utils as web_utils
from hummingbot.connector.exchange.rkex.rkex_api_order_book_data_source import RkexAPIOrderBookDataSource
from hummingbot.connector.exchange.rkex.rkex_exchange import RkexExchange
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.network_iterator import NetworkStatus


class TestRkexExchange(unittest.TestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.base_asset = "BTC"
        cls.quote_asset = "USDT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset + cls.quote_asset
        cls.api_key = "testApiKey"
        cls.api_secret_key = "testSecretKey"

    def setUp(self) -> None:
        super().setUp()

        self.log_records = []
        self.test_task: Optional[asyncio.Task] = None
        self.client_config_map = ClientConfigAdapter(ClientConfigMap())

        self.exchange = RkexExchange(
            rkex_api_key=self.api_key,
            rkex_api_secret=self.api_secret_key,
            trading_pairs=[self.trading_pair]
        )

        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(0)
        self.exchange._time_synchronizer.logger().setLevel(1)
        self.exchange._time_synchronizer.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

        self._initialize_event_loggers()

        RkexAPIOrderBookDataSource._trading_pair_symbol_map = {
            CONSTANTS.DEFAULT_DOMAIN: bidict(
                {self.ex_trading_pair: self.trading_pair})
        }

    def tearDown(self) -> None:
        self.test_task and self.test_task.cancel()
        RkexAPIOrderBookDataSource._trading_pair_symbol_map = {}
        super().tearDown()

    def _initialize_event_loggers(self):
        self.buy_order_completed_logger = EventLogger()
        self.buy_order_created_logger = EventLogger()
        self.order_cancelled_logger = EventLogger()
        self.order_failure_logger = EventLogger()
        self.order_filled_logger = EventLogger()
        self.sell_order_completed_logger = EventLogger()
        self.sell_order_created_logger = EventLogger()

        events_and_loggers = [
            (MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger),
            (MarketEvent.BuyOrderCreated, self.buy_order_created_logger),
            (MarketEvent.OrderCancelled, self.order_cancelled_logger),
            (MarketEvent.OrderFailure, self.order_failure_logger),
            (MarketEvent.OrderFilled, self.order_filled_logger),
            (MarketEvent.SellOrderCompleted, self.sell_order_completed_logger),
            (MarketEvent.SellOrderCreated, self.sell_order_created_logger)]

        for event, logger in events_and_loggers:
            self.exchange.add_listener(event, logger)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message for record in self.log_records)

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_exchange_rules_mock(self) -> Dict:
        exchange_rules = {
            "timezone": "UTC",
            "serverTime": 1000,
            "symbols": [
                {
                    "symbol": self.ex_trading_pair,
                    "status": "TRADING",
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "baseAssetPrecision": 8,
                    "quotePrecision": 8,
                    "orderTypes": ["LIMIT", "MARKET"],
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.00000100",
                            "maxPrice": "100000.00000000",
                            "tickSize": "0.00000100"
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.00100000",
                            "maxQty": "100000.00000000",
                            "stepSize": "0.00100000"
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "10.00000000"
                        }
                    ]
                }
            ]
        }
        return exchange_rules

    def _simulate_trading_rules_initialized(self):
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal(str(0.001)),
                min_price_increment=Decimal(str(0.000001)),
                min_base_amount_increment=Decimal(str(0.000001)),
                min_notional_size=Decimal(str(10))
            )
        }
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(self.get_exchange_rules_mock())

    def _validate_auth_credentials_present(self, request_call_tuple: NamedTuple):
        request_headers = request_call_tuple.kwargs["headers"]
        self.assertIn("x-api-key", request_headers)
        self.assertIn("x-timestamp", request_headers)
        self.assertIn("x-signature", request_headers)

    def test_supported_order_types(self):
        supported_types = self.exchange.supported_order_types()
        self.assertIn(OrderType.MARKET, supported_types)
        self.assertIn(OrderType.LIMIT, supported_types)
        self.assertIn(OrderType.LIMIT_MAKER, supported_types)

    def test_name(self):
        self.assertEqual("rkex", self.exchange.name)

    def test_client_order_id_max_length(self):
        self.assertEqual(CONSTANTS.MAX_ORDER_ID_LEN, self.exchange.client_order_id_max_length)

    def test_client_order_id_prefix(self):
        self.assertEqual(CONSTANTS.HBOT_ORDER_ID_PREFIX, self.exchange.client_order_id_prefix)

    @aioresponses()
    def test_check_network_success(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
        resp = {}
        mock_api.get(url, body=json.dumps(resp))

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(NetworkStatus.CONNECTED, ret)

    @aioresponses()
    def test_check_network_failure(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)
        mock_api.get(url, status=500)

        ret = self.async_run_with_timeout(coroutine=self.exchange.check_network())

        self.assertEqual(ret, NetworkStatus.NOT_CONNECTED)

    @aioresponses()
    def test_check_network_raises_cancel_exception(self, mock_api):
        url = web_utils.public_rest_url(CONSTANTS.PING_PATH_URL)

        mock_api.get(url, exception=asyncio.CancelledError)

        self.assertRaises(asyncio.CancelledError, self.async_run_with_timeout, self.exchange.check_network())

    @aioresponses()
    def test_update_trading_rules(self, mock_api):
        self.exchange._set_current_timestamp(1000)

        url = web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        exchange_rules = self.get_exchange_rules_mock()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(exchange_rules)

        mock_api.get(regex_url, body=json.dumps(exchange_rules))
        self.async_run_with_timeout(coroutine=self.exchange._update_trading_rules())

        self.assertTrue(self.trading_pair in self.exchange._trading_rules)
        trading_rule = self.exchange._trading_rules[self.trading_pair]
        self.assertEqual(Decimal("0.001"), trading_rule.min_order_size)
        self.assertEqual(Decimal("0.000001"), trading_rule.min_price_increment)

    def test_initial_status_dict(self):
        RkexAPIOrderBookDataSource._trading_pair_symbol_map = {}

        status_dict = self.exchange.status_dict

        expected_initial_dict = {
            "symbols_mapping_initialized": False,
            "order_books_initialized": False,
            "account_balance": False,
            "trading_rule_initialized": False,
            "user_stream_initialized": False,
        }

        self.assertEqual(expected_initial_dict, status_dict)
        self.assertFalse(self.exchange.ready)

    def test_get_fee_returns_default_fee(self):
        fee = self.exchange.get_fee(
            base_currency="BTC",
            quote_currency="USDT",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("50000"),
        )

        self.assertEqual(Decimal("0.001"), fee.percent)

    def test_get_fee_for_maker_order(self):
        fee = self.exchange.get_fee(
            base_currency="BTC",
            quote_currency="USDT",
            order_type=OrderType.LIMIT_MAKER,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("50000"),
        )

        # Maker fee
        self.assertEqual(Decimal("0.001"), fee.percent)

    @patch("hummingbot.connector.utils.get_tracking_nonce")
    def test_client_order_id_on_order(self, mocked_nonce):
        mocked_nonce.return_value = 9

        result = self.exchange.buy(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = f"{CONSTANTS.HBOT_ORDER_ID_PREFIX}-buy-{self.trading_pair}-9"

        self.assertEqual(result, expected_client_order_id)

        result = self.exchange.sell(
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
            price=Decimal("2"),
        )
        expected_client_order_id = f"{CONSTANTS.HBOT_ORDER_ID_PREFIX}-sell-{self.trading_pair}-9"

        self.assertEqual(result, expected_client_order_id)

    @aioresponses()
    def test_update_balances(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.ACCOUNTS_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))

        response = {
            "makerCommission": 0,
            "takerCommission": 0,
            "buyerCommission": 0,
            "sellerCommission": 0,
            "canTrade": True,
            "canWithdraw": True,
            "canDeposit": True,
            "updateTime": 123456789,
            "balances": [
                {
                    "asset": "BTC",
                    "free": "10.5",
                    "locked": "0.5"
                },
                {
                    "asset": "USDT",
                    "free": "50000.0",
                    "locked": "1000.0"
                }
            ]
        }

        mock_api.get(regex_url, body=json.dumps(response))
        self.async_run_with_timeout(self.exchange._update_balances())

        available_balances = self.exchange.available_balances
        total_balances = self.exchange.get_all_balances()

        self.assertEqual(Decimal("10.5"), available_balances["BTC"])
        self.assertEqual(Decimal("50000.0"), available_balances["USDT"])
        self.assertEqual(Decimal("11.0"), total_balances["BTC"])
        self.assertEqual(Decimal("51000.0"), total_balances["USDT"])

    def test_trading_pair_symbol_map_initialization(self):
        exchange_info = self.get_exchange_rules_mock()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(exchange_info)

        self.assertEqual(1, len(self.exchange.trading_pair_symbol_map))
        self.assertIn(self.ex_trading_pair, self.exchange.trading_pair_symbol_map)
        self.assertEqual(self.trading_pair, self.exchange.trading_pair_symbol_map[self.ex_trading_pair])

    def test_format_trading_rules(self):
        exchange_info = self.get_exchange_rules_mock()
        trading_rules = self.async_run_with_timeout(
            self.exchange._format_trading_rules(exchange_info)
        )

        self.assertEqual(1, len(trading_rules))
        trading_rule = trading_rules[0]

        self.assertEqual(self.trading_pair, trading_rule.trading_pair)
        self.assertEqual(Decimal("0.001"), trading_rule.min_order_size)
        self.assertEqual(Decimal("0.000001"), trading_rule.min_price_increment)
        self.assertEqual(Decimal("0.001"), trading_rule.min_base_amount_increment)
        self.assertEqual(Decimal("10"), trading_rule.min_notional_size)
