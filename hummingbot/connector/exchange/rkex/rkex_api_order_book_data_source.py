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
        """
        from hummingbot.connector.exchange.rkex import rkex_web_utils as web_utils
        from hummingbot.core.web_assistant.connections.data_types import RESTMethod

        # Extract base and quote currency from trading pair (e.g., "SOL-USDC" -> "SOL", "USDC")
        base, quote = trading_pair.split("-")

        try:
            rest_assistant = await self._api_factory.get_rest_assistant()

            # Call POST /orderBook endpoint with baseCoin, quoteCoin, and limit
            request_data = {
                "baseCoin": base,
                "quoteCoin": quote,
                "limit": 100
            }

            url = web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_PATH_URL)
            self.logger().info(f"Requesting order book for {trading_pair} with params: {request_data}")
            self.logger().info(f"Order book URL: {url}")

            response = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.POST,
                data=request_data,
                throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
                is_auth_required=False
            )

            self.logger().info(f"Order book response received: {response}")

            # Convert response format from API to hummingbot format
            # API returns: {"asks": [{"price": 1.0003, "size": 1}, ...], "bids": [{"price": 0.999, "size": 1}, ...]}
            # We need: {"bids": [[0.999, 1], ...], "asks": [[1.0003, 1], ...]}

            bids = [[float(item["price"]), float(item["size"])] for item in response.get("bids", [])]
            asks = [[float(item["price"]), float(item["size"])] for item in response.get("asks", [])]

            # Sort bids descending (highest first), asks ascending (lowest first)
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            self.logger().info(
                f"Fetched order book for {trading_pair}: {len(bids)} bids, {len(asks)} asks"
            )

            if len(bids) == 0:
                self.logger().warning(f"WARNING: Order book for {trading_pair} has NO BIDS!")
            if len(asks) == 0:
                self.logger().warning(f"WARNING: Order book for {trading_pair} has NO ASKS!")

            if len(bids) > 0:
                self.logger().info(f"Best bid for {trading_pair}: {bids[0]}")
            if len(asks) > 0:
                self.logger().info(f"Best ask for {trading_pair}: {asks[0]}")

            result = {
                "lastUpdateId": int(time.time() * 1000),
                "bids": bids,
                "asks": asks
            }

            self.logger().info(f"Returning snapshot with {len(bids)} bids, {len(asks)} asks, first bid: {bids[0] if bids else 'none'}, first ask: {asks[0] if asks else 'none'}")

            return result

        except Exception as e:
            self.logger().error(
                f"Failed to fetch order book for {trading_pair}: {e}. "
                f"Returning empty order book.",
                exc_info=True
            )
            return {
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
        Since RKEX API does not support WebSocket, poll order book snapshots via REST API.
        This replaces the WebSocket subscription mechanism.
        """
        self.logger().info(
            "WebSocket order book streaming not available on RKEX API. "
            "Using REST API polling for order book updates (every 5 seconds)."
        )

        while True:
            try:
                # Fetch order book snapshots for all trading pairs
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot_msg = await self._order_book_snapshot(trading_pair)
                        self._message_queue[self._snapshot_messages_queue_key].put_nowait(snapshot_msg)
                    except Exception as e:
                        self.logger().error(f"Error fetching order book for {trading_pair}: {e}", exc_info=True)

                # Poll every 5 seconds
                await asyncio.sleep(5.0)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Error in order book polling loop: {e}", exc_info=True)
                await asyncio.sleep(5.0)

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

    async def _parse_order_book_snapshot_message(self, raw_message: OrderBookMessage, message_queue: asyncio.Queue):
        """
        Parse order book snapshot messages and add them to the output queue.
        Since we already create OrderBookMessage objects in _order_book_snapshot,
        we just pass them through.
        """
        message_queue.put_nowait(raw_message)

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
