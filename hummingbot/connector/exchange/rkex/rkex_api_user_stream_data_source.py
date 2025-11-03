import asyncio
import time
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.rkex import rkex_constants as CONSTANTS, rkex_web_utils as web_utils
from hummingbot.connector.exchange.rkex.rkex_auth import RkexAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.rkex.rkex_exchange import RkexExchange


class RkexAPIUserStreamDataSource(UserStreamTrackerDataSource):

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0
    LISTEN_KEY_RETRY_INTERVAL = 5.0
    MAX_RETRIES = 3

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: RkexAuth,
                 trading_pairs: List[str],
                 connector: 'RkexExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: RkexAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._current_listen_key = None
        self._last_listen_key_ping_ts = None
        self._manage_listen_key_task = None
        self._listen_key_initialized_event = asyncio.Event()
        self._user_stream_not_available_ts = None  # Timestamp when we determined user stream is unavailable

    @property
    def last_recv_time(self) -> float:
        """
        Returns the time of the last received message.
        For RKEX, since user stream (WebSocket) is not available, we return a timestamp
        indicating the REST polling is active instead.
        """
        if self._user_stream_not_available_ts is not None:
            # User stream not available, but REST polling is active
            return self._user_stream_not_available_ts
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        """
        Creates a new WSAssistant instance.
        """
        # Always create a new assistant to avoid connection issues
        return await self._api_factory.get_ws_assistant()

    async def _get_listen_key(self, max_retries: int = MAX_RETRIES) -> str:
        """
        Fetches a listen key from the exchange with retries and backoff.

        :param max_retries: Maximum number of retry attempts
        :return: Valid listen key string

        NOTE: RKEX API may not support user stream endpoints. This method will fail gracefully.
        """
        retry_count = 0
        backoff_time = 1.0
        timeout = 5.0

        rest_assistant = await self._api_factory.get_rest_assistant()
        while True:
            try:
                data = await rest_assistant.execute_request(
                    url=web_utils.private_rest_url(path_url=CONSTANTS.USER_STREAM_PATH_URL, domain=self._domain),
                    method=RESTMethod.POST,
                    throttler_limit_id=CONSTANTS.USER_STREAM_PATH_URL,
                    is_auth_required=True,
                    timeout=timeout,
                )
                return data.get("listenKey", "")
            except asyncio.CancelledError:
                raise
            except Exception as exception:
                error_msg = str(exception).lower()
                # If endpoint doesn't exist (404) or method not allowed (405), stop retrying
                if "404" in error_msg or "405" in error_msg or "not found" in error_msg:
                    self.logger().warning(
                        f"User stream endpoint not available on RKEX API. "
                        f"Order updates will be retrieved via polling instead. Error: {exception}"
                    )
                    # Return empty string to signal that user stream is not available
                    return ""

                retry_count += 1
                if retry_count > max_retries:
                    self.logger().warning(
                        f"Error fetching user stream listen key after {max_retries} retries. "
                        f"User stream disabled. Error: {exception}"
                    )
                    return ""

                self.logger().warning(f"Retry {retry_count}/{max_retries} fetching user stream listen key. Error: {repr(exception)}")
                await self._sleep(backoff_time)
                backoff_time *= 2

    async def _ping_listen_key(self) -> bool:
        """
        Sends a ping to keep the listen key alive.

        :return: True if successful, False otherwise
        """
        rest_assistant = await self._api_factory.get_rest_assistant()
        try:
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(path_url=CONSTANTS.USER_STREAM_PATH_URL, domain=self._domain),
                params={"listenKey": self._current_listen_key},
                method=RESTMethod.PUT,
                return_err=True,
                throttler_limit_id=CONSTANTS.USER_STREAM_PATH_URL,
                is_auth_required=True
            )

            if "code" in data:
                self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {data}")
                return False

        except asyncio.CancelledError:
            raise
        except Exception as exception:
            self.logger().warning(f"Failed to refresh the listen key {self._current_listen_key}: {exception}")
            return False

        return True

    async def _manage_listen_key_task_loop(self):
        """
        Background task that manages the listen key lifecycle:
        1. Obtains a new listen key if needed
        2. Periodically refreshes the listen key to keep it active
        3. Handles errors and resets state when necessary

        NOTE: If user stream is not available, this will set the event and exit gracefully.
        """
        self.logger().info("Starting listen key management task...")
        while True:
            try:
                now = int(time.time())

                # Initialize listen key if needed
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now

                    # If listen key is empty, user stream is not available
                    if not self._current_listen_key:
                        self.logger().info(
                            "User stream not available on RKEX API. "
                            "Order updates will be retrieved via REST API polling."
                        )
                        # Set timestamp to mark user stream as "initialized" (using REST fallback)
                        self._user_stream_not_available_ts = time.time()
                        self._listen_key_initialized_event.set()
                        # Exit the loop - no user stream support
                        return

                    self._listen_key_initialized_event.set()
                    self.logger().info(f"Successfully obtained listen key {self._current_listen_key}")

                # Refresh listen key periodically
                if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    success = await self._ping_listen_key()
                    if success:
                        self.logger().info(f"Successfully refreshed listen key {self._current_listen_key}")
                        self._last_listen_key_ping_ts = now
                    else:
                        self.logger().error(f"Failed to refresh listen key {self._current_listen_key}. Getting new key...")
                        self._current_listen_key = None
                        continue
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
            except asyncio.CancelledError:
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                raise
            except Exception as e:
                self.logger().error(f"Error occurred renewing listen key ... {e}")
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)

    async def _ensure_listen_key_task_running(self):
        """
        Ensures the listen key management task is running.
        """
        # If task is already running, do nothing
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            return

        # Cancel old task if it exists and is done (failed)
        if self._manage_listen_key_task is not None:
            self._manage_listen_key_task.cancel()

        # Create new task
        self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.

        This method ensures the listen key is ready before connecting.

        NOTE: If user stream is not available, this returns None and the base class
        will fall back to REST API polling for order updates.
        """
        # Make sure the listen key management task is running
        await self._ensure_listen_key_task_running()

        # Wait for the listen key to be initialized
        await self._listen_key_initialized_event.wait()

        # If listen key is empty, user stream is not available
        if not self._current_listen_key:
            self.logger().info(
                "User stream WebSocket not available on RKEX API. "
                "Using REST API polling for order updates."
            )
            # Return None to signal that WebSocket is not available
            # The base class should handle this gracefully
            raise NotImplementedError("User stream WebSocket not available on RKEX API")

        # Get a websocket assistant and connect it
        ws = await self._get_ws_assistant()
        url = f"{CONSTANTS.WSS_URL}/{self._current_listen_key}"

        self.logger().info(f"Connecting to user stream with listen key {self._current_listen_key}")
        await ws.connect(ws_url=url, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        self.logger().info("Successfully connected to user stream")

        return ws

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Override to disable user stream when not available.

        RKEX API does not support user stream WebSocket, so we skip this entirely
        and rely on REST API polling for order updates.
        """
        # Start the listen key task to check if user stream is available
        await self._ensure_listen_key_task_running()
        await self._listen_key_initialized_event.wait()

        # If user stream is not available, just log and return (no retry loop)
        if not self._current_listen_key:
            self.logger().info(
                "User stream WebSocket not available on RKEX API. "
                "Order updates will be retrieved via REST API polling. "
                "This is normal for this exchange."
            )
            # Keep this method alive but do nothing - prevents retry loop
            while True:
                await asyncio.sleep(60)
        else:
            # If somehow we do have a listen key, use parent implementation
            await super().listen_for_user_stream(output)

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        Rkex does not require any channel subscription.

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        pass

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        """
        Handles websocket disconnection by cleaning up resources.

        :param websocket_assistant: The websocket assistant that was disconnected
        """
        self.logger().info("User stream interrupted. Cleaning up...")

        # Cancel listen key management task first
        if self._manage_listen_key_task and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass  # Ignore any exception from the task
            self._manage_listen_key_task = None

        # Disconnect the websocket if it exists
        websocket_assistant and await websocket_assistant.disconnect()
        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
