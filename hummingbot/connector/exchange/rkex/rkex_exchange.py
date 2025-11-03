import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS, rkex_utils, rkex_web_utils as web_utils
from hummingbot.connector.exchange.rkex.rkex_api_order_book_data_source import RkexAPIOrderBookDataSource
from hummingbot.connector.exchange.rkex.rkex_api_user_stream_data_source import RkexAPIUserStreamDataSource
from hummingbot.connector.exchange.rkex.rkex_auth import RkexAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class RkexExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 rkex_api_key: str,
                 rkex_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = rkex_api_key
        self.secret_key = rkex_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_rkex_timestamp = 1.0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @staticmethod
    def rkex_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(rkex_type: str) -> OrderType:
        return OrderType[rkex_type]

    @property
    def authenticator(self):
        return RkexAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "rkex"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        """
        Returns a list of price data for all trading pairs.
        Since the ticker endpoints are not available, we construct the response from exchange info.
        """
        # The ticker endpoints don't exist on this exchange, so we return an empty list
        # or construct a minimal response from available data
        try:
            exchange_info = await self._api_get(path_url=CONSTANTS.EXCHANGE_INFO_PATH_URL)
            pairs_prices = []
            for symbol_data in exchange_info.get("symbols", []):
                if symbol_data.get("status") == "TRADING":
                    pairs_prices.append({
                        "symbol": symbol_data["symbol"],
                        "price": "0"  # Price not available without ticker endpoint
                    })
            return pairs_prices
        except Exception:
            # If exchange info fails, return empty list
            return []

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return RkexAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return RkexAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        """
        Places an order on RKEX exchange.

        RKEX API format:
        - market: "SOL/USDT" (uses "/" separator)
        - type: "buy" or "sell" (lowercase, represents side not order type)
        - bid: true/false (boolean)
        - size: float (amount)
        - price: float (limit price)
        - stopPrice: float (stop price, 0 for normal orders)
        - currency: "SOL" (base asset)
        """
        self.logger().info(
            f"_place_order called: order_id={order_id}, trading_pair={trading_pair}, "
            f"amount={amount}, trade_type={trade_type}, order_type={order_type}, price={price}"
        )

        order_result = None
        amount_str = f"{amount:f}"

        # Get the exchange symbol (e.g., "SOL/USDT" with "/" separator)
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        self.logger().info(f"Exchange symbol for {trading_pair}: {symbol}")

        # Extract base and quote currency from trading pair
        base, quote = trading_pair.split("-")

        # RKEX API uses "bid" boolean and "type" as side
        is_bid = trade_type is TradeType.BUY
        side_str = "buy" if is_bid else "sell"

        # RKEX API format based on actual API response
        api_params = {
            "market": symbol,
            "type": side_str,  # "buy" or "sell" (side, not order type!)
            "bid": is_bid,  # true for buy, false for sell
            "size": float(amount_str),
            "currency": base,  # Base asset (SOL, not USDT)
        }

        self.logger().info(f"Order parameters prepared (initial): {api_params}")

        # Add price based on order type
        if order_type in [OrderType.LIMIT, OrderType.LIMIT_MAKER]:
            # Limit orders: use the specified price
            price_str = f"{price:f}"
            api_params["price"] = float(price_str)
            # stopPrice must be positive - set it to the order price for normal orders
            api_params["stopPrice"] = float(price_str)
        elif order_type == OrderType.MARKET:
            # Market orders: get current best price from order book
            # For market buy: use best ask price
            # For market sell: use best bid price
            try:
                # Try to get a reasonable price from the order book
                order_book = self.get_order_book(trading_pair)
                if is_bid:
                    # Market buy: use best ask (slightly above to ensure fill)
                    best_ask = float(order_book.ask_entries()[0].price) if order_book.ask_entries() else float(price)
                    market_price = best_ask * 1.01 if best_ask > 0 else float(price)
                else:
                    # Market sell: use best bid (slightly below to ensure fill)
                    best_bid = float(order_book.bid_entries()[0].price) if order_book.bid_entries() else float(price)
                    market_price = best_bid * 0.99 if best_bid > 0 else float(price)

                api_params["price"] = market_price
                api_params["stopPrice"] = market_price  # stopPrice must be positive
                self.logger().info(f"Market order price set to {market_price} based on order book")
            except Exception as e:
                self.logger().warning(f"Could not get order book price for market order: {e}. Using provided price.")
                api_params["price"] = float(price)
                api_params["stopPrice"] = float(price)
        else:
            # For other order types, use provided price
            api_params["price"] = float(price)
            api_params["stopPrice"] = float(price)

        try:
            self.logger().info(f"Sending order request to {CONSTANTS.ORDER_PATH_URL}")

            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)

            self.logger().info(f"Order API response: {order_result}")

            # Response format: {"success": true, "message": "...", "order": {...}}
            # The actual order data is nested inside "order" field
            if not order_result.get("success"):
                # If not successful, raise error
                error_msg = order_result.get("message", "Unknown error")
                self.logger().error(f"Order placement failed: {error_msg}")
                raise IOError(f"Order placement failed: {error_msg}")

            transact_time = self._time_synchronizer.time()

            # The placement response doesn't include order ID
            # We need to query active orders to find our order
            await asyncio.sleep(0.5)  # Small delay to ensure order is in the system

            try:
                active_orders = await self._api_get(
                    path_url=CONSTANTS.ACTIVE_ORDERS_PATH_URL,
                    is_auth_required=True)

                # Find our order by matching market, side, price, and size
                for order in active_orders:
                    if (order.get("market") == symbol
                            and order.get("bids") == is_bid
                            and abs(float(order.get("price", 0)) - float(api_params["price"])) < 0.000001
                            and abs(float(order.get("size", 0)) - float(api_params["size"])) < 0.000001):
                        # Found our order
                        o_id = str(order.get("id"))
                        # Use the order's timestamp if available
                        timestamp_value = order.get("timestamp")
                        if timestamp_value and isinstance(timestamp_value, str):
                            transact_time = int(timestamp_value) / 1e9
                        self.logger().info(f"Found order in active orders with ID: {o_id}")
                        break
                else:
                    # Order not found in active orders yet
                    # Use a temporary ID and let polling update it later
                    self.logger().warning(
                        "Order placed successfully but not found in active orders yet. "
                        "Using temporary ID."
                    )
                    o_id = f"temp_{int(transact_time * 1000)}"
            except Exception as e:
                self.logger().warning(f"Could not fetch order ID from active orders: {e}. Using temporary ID.")
                o_id = f"temp_{int(transact_time * 1000)}"

            self.logger().info(f"Order placement completed: order_id={o_id}, transact_time={transact_time}")

        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        # API uses DELETE /order/:orderId format, where orderId is the exchange order ID
        # We need to use the exchange_order_id from the tracked order
        if tracked_order.exchange_order_id is None:
            # If we don't have the exchange order ID yet, we can't cancel
            raise ValueError(f"Cannot cancel order {order_id}: exchange order ID not available")

        # The API endpoint is /order/:orderId
        path_url = f"{CONSTANTS.ORDER_PATH_URL}/{tracked_order.exchange_order_id}"

        cancel_result = await self._api_delete(
            path_url=path_url,
            is_auth_required=True)

        if cancel_result.get("status") == "CANCELED":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
            "symbol": "ETHBTC",
            "baseAssetPrecision": 8,
            "quotePrecision": 8,
            "orderTypes": ["LIMIT", "MARKET"],
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00000100",
                    "maxPrice": "100000.00000000",
                    "tickSize": "0.00000100"
                }, {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00100000",
                    "maxQty": "100000.00000000",
                    "stepSize": "0.00100000"
                }, {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "0.00100000"
                }
            ]
        }
        """
        trading_pair_rules = exchange_info_dict.get("symbols", [])
        retval = []
        for rule in filter(rkex_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                # Convert symbol from "SOLUSDT" to "SOL/USDT" format (with slash)
                # which matches our symbol mapping
                base_asset = rule.get("quoteAsset")  # Swapped!
                quote_asset = rule.get("baseAsset")  # Swapped!
                exchange_symbol_with_slash = f"{base_asset}/{quote_asset}"

                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=exchange_symbol_with_slash)
                filters = rule.get("filters", [])

                # Handle case where filters might be empty or missing
                price_filters = [f for f in filters if f.get("filterType") == "PRICE_FILTER"]
                lot_size_filters = [f for f in filters if f.get("filterType") == "LOT_SIZE"]
                min_notional_filters = [f for f in filters if f.get("filterType") in ["MIN_NOTIONAL", "NOTIONAL"]]

                # Use default values if filters are not provided
                # Note: The API returns baseAsset and quoteAsset swapped, so we swap the precisions too
                base_precision = rule.get("quoteAssetPrecision", 8)
                quote_precision = rule.get("baseAssetPrecision", 8)

                if price_filters:
                    price_filter = price_filters[0]
                    tick_size = Decimal(price_filter.get("tickSize", f"1e-{quote_precision}"))
                else:
                    tick_size = Decimal(f"1e-{quote_precision}")

                if lot_size_filters:
                    lot_size_filter = lot_size_filters[0]
                    min_order_size = Decimal(lot_size_filter.get("minQty", f"1e-{base_precision}"))
                    step_size = Decimal(lot_size_filter.get("stepSize", f"1e-{base_precision}"))
                else:
                    min_order_size = Decimal(f"1e-{base_precision}")
                    step_size = Decimal(f"1e-{base_precision}")

                if min_notional_filters:
                    min_notional_filter = min_notional_filters[0]
                    min_notional = Decimal(min_notional_filter.get("minNotional", "0.001"))
                else:
                    min_notional = Decimal("0.001")

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=tick_size,
                                min_base_amount_increment=step_size,
                                min_notional_size=min_notional))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("e")
                # Refer to https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
                # As per the order update section in Binance the ID of the order being canceled is under the "C" key
                if event_type == "executionReport":
                    execution_type = event_message.get("x")
                    if execution_type != "CANCELED":
                        client_order_id = event_message.get("c")
                    else:
                        client_order_id = event_message.get("C")

                    if execution_type == "TRADE":
                        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                        if tracked_order is not None:
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=event_message["N"],
                                flat_fees=[TokenAmount(amount=Decimal(event_message["n"]), token=event_message["N"])]
                            )
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["t"]),
                                client_order_id=client_order_id,
                                exchange_order_id=str(event_message["i"]),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(event_message["l"]),
                                fill_quote_amount=Decimal(event_message["l"]) * Decimal(event_message["L"]),
                                fill_price=Decimal(event_message["L"]),
                                fill_timestamp=event_message["T"] * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)

                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message["E"] * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[event_message["X"]],
                            client_order_id=client_order_id,
                            exchange_order_id=str(event_message["i"]),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "outboundAccountPosition":
                    balances = event_message["B"]
                    for balance_entry in balances:
                        asset_name = balance_entry["a"]
                        free_balance = Decimal(balance_entry["f"])
                        total_balance = Decimal(balance_entry["f"]) + Decimal(balance_entry["l"])
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_order_fills_from_trades(self):
        """
        RKEX API does not support /myTrades endpoint (returns 404).

        Order fills are detected through order status changes (PENDING → FILLED)
        via the _request_order_status() method which polls /order/active-orders
        and /order/all-orders.

        This method is disabled for RKEX.
        """
        # Do nothing - fills are detected via order status changes
        pass

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Fetches all trade updates for a specific order.

        RKEX API does not support /myTrades endpoint (returns 404).
        Fills are detected through order status changes via _request_order_status().

        Returns empty list - fills are tracked via order status polling.
        """
        # Always return empty list - /myTrades endpoint doesn't exist on RKEX
        # Order fills are detected via status changes in _request_order_status()
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Requests order status from the exchange.

        RKEX active orders response format:
        {
          "id": "uuid",
          "market": "SOL/USDT",
          "status": "PENDING",
          "bids": true,
          "orderType": "BUY",
          "orderPlacedType": "LIMIT",
          "price": 1,
          "size": 0.1,
          "timestamp": "1762156744294746232"
        }
        """
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = None

        # First try active orders endpoint (more efficient)
        try:
            active_orders = await self._api_get(
                path_url=CONSTANTS.ACTIVE_ORDERS_PATH_URL,
                is_auth_required=True)

            # Filter for our specific order
            for order in active_orders:
                # Match by market first (uses "/" format like "SOL/USDT")
                if order.get("market") != trading_pair:
                    continue
                # Match by exchange order ID (field is "id" not "orderId")
                # Note: clientOrderId is not in the response, so we can only match by exchange ID
                if (str(order.get("id")) == tracked_order.exchange_order_id):
                    updated_order_data = order
                    break
        except Exception as e:
            self.logger().debug(f"Could not fetch from active orders: {e}, trying all orders")

        # If not found in active orders, try all orders (might have different format)
        if updated_order_data is None:
            try:
                all_orders = await self._api_get(
                    path_url=CONSTANTS.ALL_ORDERS_PATH_URL,
                    params={"symbol": trading_pair},
                    is_auth_required=True)

                for order in all_orders:
                    # Try matching by various ID fields
                    order_id_matches = (
                        str(order.get("id")) == tracked_order.exchange_order_id
                        or str(order.get("orderId")) == tracked_order.exchange_order_id
                        or str(order.get("_id")) == tracked_order.exchange_order_id
                    )
                    # Also check market/symbol field
                    market_matches = (
                        order.get("market") == trading_pair
                        or order.get("symbol") == trading_pair
                    )

                    if order_id_matches and market_matches:
                        updated_order_data = order
                        break
            except Exception as e:
                self.logger().debug(f"Could not fetch from all orders: {e}")

        if updated_order_data is None:
            raise ValueError(
                f"Order {tracked_order.client_order_id} (exchange ID: {tracked_order.exchange_order_id}) "
                f"not found in orders response"
            )

        # Parse order status
        order_status = updated_order_data.get("status", "UNKNOWN").upper()
        new_state = CONSTANTS.ORDER_STATE.get(order_status, OrderState.FAILED)

        # Get order ID (field is "id" in active orders response)
        order_id = str(updated_order_data.get("id") or updated_order_data.get("orderId") or updated_order_data.get("_id"))

        # Parse timestamp (might be a large string number in nanoseconds)
        timestamp_value = updated_order_data.get("timestamp", updated_order_data.get("updateTime", updated_order_data.get("time", 0)))
        if isinstance(timestamp_value, str):
            # Convert from nanoseconds to seconds
            timestamp = int(timestamp_value) / 1e9
        else:
            # Already in milliseconds, convert to seconds
            timestamp = timestamp_value * 1e-3 if timestamp_value > 0 else self._time_synchronizer.time()

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp,
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        """
        Get account balances from the exchange.

        NOTE: The /balance endpoint returns a list of currency objects directly,
        not a Binance-style dict with "balances" key.

        Response format:
        [
            {
                "currency": "USDC",
                "amount": 10,          # Total balance
                "lockedAmount": 0,     # Locked balance
                ...
            }
        ]
        """
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        # Get balances - API returns a list directly
        balances = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True)

        # Process each balance entry
        for balance_entry in balances:
            asset_name = balance_entry["currency"]
            total_balance = Decimal(str(balance_entry.get("amount", 0)))
            locked_balance = Decimal(str(balance_entry.get("lockedAmount", 0)))
            free_balance = total_balance - locked_balance

            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        # Remove assets that are no longer in the account
        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(rkex_utils.is_exchange_information_valid, exchange_info["symbols"]):
            # Note: The API returns baseAsset and quoteAsset swapped, so we swap them back here
            # Also: exchangeInfo uses "SOLUSDT" format, but orders API uses "SOL/USDT" format
            # We need to convert the symbol to include "/" separator
            # symbol_data["symbol"] is e.g., "SOLUSDT"
            base_asset = symbol_data["quoteAsset"]   # Swapped!
            quote_asset = symbol_data["baseAsset"]   # Swapped!

            # Create the exchange symbol in "SOL/USDT" format (with slash)
            # This is the format used by the orders API
            exchange_symbol_with_slash = f"{base_asset}/{quote_asset}"

            # Map to Hummingbot format "SOL-USDT" (with hyphen)
            hb_trading_pair = combine_to_hb_trading_pair(base=base_asset, quote=quote_asset)

            mapping[exchange_symbol_with_slash] = hb_trading_pair

            # self.logger().info(f"Mapped exchange symbol '{exchange_symbol_with_slash}' to '{hb_trading_pair}'")

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Get the last traded price for a trading pair.

        Since ticker endpoints are not available, we calculate from:
        1. Order book mid-price (if both sides available)
        2. Order book one-sided price (if only bids or asks)
        3. Last filled order price
        4. Fallback to reasonable default (100.0)
        """
        try:
            # Try to get price from order book
            order_book = self.get_order_book(trading_pair)
            if order_book:
                has_bids = bool(order_book.bid_entries())
                has_asks = bool(order_book.ask_entries())

                if has_bids and has_asks:
                    # Both sides available - use mid-price
                    best_bid = float(order_book.bid_entries()[0].price)
                    best_ask = float(order_book.ask_entries()[0].price)
                    mid_price = (best_bid + best_ask) / 2.0
                    self.logger().info(f"Using order book mid-price {mid_price} for {trading_pair}")
                    return mid_price
                elif has_bids:
                    # Only bids available - use best bid
                    best_bid = float(order_book.bid_entries()[0].price)
                    self.logger().info(f"Using best bid price {best_bid} for {trading_pair} (no asks available)")
                    return best_bid
                elif has_asks:
                    # Only asks available - use best ask
                    best_ask = float(order_book.ask_entries()[0].price)
                    self.logger().info(f"Using best ask price {best_ask} for {trading_pair} (no bids available)")
                    return best_ask
        except Exception as e:
            self.logger().debug(f"Could not get price from order book: {e}")

        try:
            # Fallback: get price from recent filled orders
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            all_orders = await self._api_get(
                path_url=CONSTANTS.ALL_ORDERS_PATH_URL,
                params={"symbol": symbol},
                is_auth_required=True)

            # Find the most recent filled order
            for order in all_orders:
                if order.get("status") == "FILLED" and order.get("meanMatchPrice"):
                    price = float(order["meanMatchPrice"])
                    self.logger().info(f"Using last filled order price {price} for {trading_pair}")
                    return price

                # Also check active orders for price reference
                if order.get("status") == "PENDING" and order.get("price"):
                    price = float(order["price"])
                    if price > 0:
                        self.logger().info(f"Using pending order price {price} for {trading_pair}")
                        return price
        except Exception as e:
            self.logger().debug(f"Could not get price from orders: {e}")

        # Last resort: return a reasonable default price for SOL/USDT
        # This allows the strategy to start even without market data
        default_price = 100.0
        self.logger().warning(
            f"No price data available for {trading_pair}, using default price {default_price}. "
            f"Strategy will create orders around this price."
        )
        return default_price
