import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS
from hummingbot.connector.exchange.rkex.rkex_order_book import RkexOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.rkex.rkex_exchange import RkexExchange


class RkexAPIOrderBookDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'RkexExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.DIFF_EVENT_TYPE
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)

        NOTE: The /depth endpoint is not implemented on this exchange API.
        We build the order book from active orders instead.
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        # Get all active orders to build the order book
        try:
            # Use the active-orders endpoint to get current order book
            from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS, rkex_web_utils as web_utils
            from hummingbot.core.web_assistant.connections.data_types import RESTMethod

            rest_assistant = await self._api_factory.get_rest_assistant()
            all_active_orders = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(path_url=CONSTANTS.ACTIVE_ORDERS_PATH_URL),
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.ACTIVE_ORDERS_PATH_URL,
                is_auth_required=True
            )

            # Filter orders for this trading pair and separate into bids/asks
            bids = []
            asks = []

            self.logger().info(f"Processing {len(all_active_orders)} active orders for symbol matching: {symbol}")

            for order in all_active_orders:
                order_market = order.get("market", "")

                # Match symbol - handle both "SOL/USDT" and "SOL-USDT" formats
                symbol_match = (
                    order_market == symbol or
                    order_market == symbol.replace("-", "/") or
                    order_market.replace("/", "-") == symbol
                )

                if not symbol_match:
                    self.logger().debug(f"Skipping order for different market: {order_market} (looking for {symbol})")
                    continue

                # Skip if order doesn't have required fields
                if "price" not in order or "size" not in order:
                    self.logger().debug(f"Skipping order without price/size: {order}")
                    continue

                price = float(order["price"])
                size = float(order["size"])

                # Skip orders with 0 size (already filled)
                if size <= 0:
                    self.logger().debug(f"Skipping order with 0 size: {order}")
                    continue

                # Determine if this is a bid or ask
                # "bids": true means buy order (bid)
                # "bids": false means sell order (ask)
                is_bid = order.get("bids", order.get("bid", True))

                if is_bid:
                    bids.append([price, size])
                    self.logger().debug(f"Added bid: price={price}, size={size}")
                else:
                    asks.append([price, size])
                    self.logger().debug(f"Added ask: price={price}, size={size}")

            # Sort bids descending (highest first), asks ascending (lowest first)
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            self.logger().info(
                f"Built order book for {trading_pair} from active orders: "
                f"{len(bids)} bids, {len(asks)} asks"
            )

            return {
                "symbol": symbol,
                "lastUpdateId": int(time.time() * 1000),
                "bids": bids,
                "asks": asks
            }

        except Exception as e:
            self.logger().warning(
                f"Could not build order book from active orders for {trading_pair}: {e}. "
                f"Returning empty order book."
            )
            return {
                "symbol": symbol,
                "lastUpdateId": 0,
                "bids": [],
                "asks": []
            }

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_params = []
            depth_params = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_params.append(f"{symbol.lower()}@trade")
                depth_params.append(f"{symbol.lower()}@depth@100ms")
            payload = {
                "method": "SUBSCRIBE",
                "params": trade_params,
                "id": 1
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload)

            payload = {
                "method": "SUBSCRIBE",
                "params": depth_params,
                "id": 2
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def listen_for_subscriptions(self):
        """
        Override to disable WebSocket subscriptions when not available.

        RKEX API does not support WebSocket for order book updates, so we skip this entirely
        and rely on REST API polling for order book snapshots.
        """
        self.logger().info(
            "WebSocket order book streaming not available on RKEX API. "
            "Order book will be updated via REST API polling. "
            "This is normal for this exchange."
        )
        # Keep this method alive but do nothing - prevents retry loop
        while True:
            await asyncio.sleep(60)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        This method is not used since WebSocket is not available on RKEX API.
        Keeping it for compatibility but it will not be called.
        """
        raise NotImplementedError("WebSocket not available on RKEX API")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = RkexOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            trade_message = RkexOrderBook.trade_message_from_exchange(
                raw_message, {"trading_pair": trading_pair})
            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if "result" not in raw_message:
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["s"])
            order_book_message: OrderBookMessage = RkexOrderBook.diff_message_from_exchange(
                raw_message, time.time(), {"trading_pair": trading_pair})
            message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if "result" not in event_message:
            event_type = event_message.get("e")
            channel = (self._diff_messages_queue_key if event_type == CONSTANTS.DIFF_EVENT_TYPE
                       else self._trade_messages_queue_key)
        return channel
