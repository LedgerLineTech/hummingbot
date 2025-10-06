from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = ""

HBOT_ORDER_ID_PREFIX = "x-MG43PCSN"
MAX_ORDER_ID_LEN = 32

# Base URL
REST_URL = "https://apiengine.demoapps.space/"
WSS_URL = "wss://apiengine.demoapps.space/ws"

PUBLIC_API_VERSION = ""
PRIVATE_API_VERSION = ""

# Public API endpoints
TICKER_PRICE_CHANGE_PATH_URL = "ticker/price"
TICKER_BOOK_PATH_URL = "ticker/bookTicker"
PRICES_PATH_URL = "ticker/price"
EXCHANGE_INFO_PATH_URL = "exchangeInfo"
PING_PATH_URL = "ping"
SNAPSHOT_PATH_URL = "depth"
SERVER_TIME_PATH_URL = "time"

# Private API endpoints
ACCOUNTS_PATH_URL = "account"
MY_TRADES_PATH_URL = "myTrades"
ORDER_PATH_URL = "order"
ALL_ORDERS_PATH_URL = "order/all-orders"
ACTIVE_ORDERS_PATH_URL = "order/active-orders"
USER_STREAM_PATH_URL = "userDataStream"

# Rate limit endpoints
RATE_LIMITS_PATH_URL = "ratelimits"
RATE_LIMITS_ENDPOINTS_PATH_URL = "ratelimits/endpoints"
RATE_LIMITS_DOCUMENTATION_PATH_URL = "ratelimits/documentation"

WS_HEARTBEAT_TIME_INTERVAL = 30

# API Engine params
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"
TIME_IN_FORCE_IOC = "IOC"
TIME_IN_FORCE_FOK = "FOK"

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

# API Engine rate limits based on documentation
GLOBAL_IP_LIMIT = 1200  # 1200 requests per minute per IP
GLOBAL_USER_LIMIT = 1200  # 1200 requests per minute per authenticated user
BURST_LIMIT = 10  # 10 requests per second burst limit

# Order States - based on API response
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "OPEN": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIAL": OrderState.PARTIALLY_FILLED,
    "CANCELLED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
    "COMPLETED": OrderState.FILLED,  # FILLED is completed
}

# Websocket event types
DIFF_EVENT_TYPE = "depthUpdate"
TRADE_EVENT_TYPE = "trade"

RATE_LIMITS = [
    # Global limits
    RateLimit(limit_id=REQUEST_WEIGHT, limit=GLOBAL_IP_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id=RAW_REQUESTS, limit=GLOBAL_USER_LIMIT, time_interval=ONE_MINUTE),
    RateLimit(limit_id="BURST", limit=BURST_LIMIT, time_interval=ONE_SECOND),

    # Endpoint-specific limits based on API documentation
    RateLimit(limit_id=PING_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDER_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ACTIVE_ORDERS_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ALL_ORDERS_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=f"{ORDER_PATH_URL}/:orderId", limit=5, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=10, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=RATE_LIMITS_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=RATE_LIMITS_ENDPOINTS_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=RATE_LIMITS_DOCUMENTATION_PATH_URL, limit=1200, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]

ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"