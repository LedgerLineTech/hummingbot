# Rkex Connector Tests

This directory contains comprehensive tests for the Rkex exchange connector for Hummingbot.

## Test Files

### 1. `test_rkex_auth.py`
Tests authentication mechanism with HMAC-SHA256 signatures.

**Test Coverage:**
- REST authentication for GET requests
- REST authentication for POST requests
- Signature generation and consistency
- Request parameter handling
- WebSocket authentication (passthrough)
- Timestamp formatting (milliseconds)

**Run:** `pytest test_rkex_auth.py -v`

### 2. `test_rkex_utils.py`
Tests utility functions for exchange information validation.

**Test Coverage:**
- Exchange status validation (TRADING/BREAK)
- Symbol filtering
- Default status handling

**Run:** `pytest test_rkex_utils.py -v`

### 3. `test_rkex_web_utils.py`
Tests URL construction for public and private endpoints.

**Test Coverage:**
- Public REST URL construction
- Private REST URL construction
- All endpoint URL validation against live API

**Run:** `pytest test_rkex_web_utils.py -v`

### 4. `test_rkex_exchange.py`
Comprehensive exchange functionality tests.

**Test Coverage:**
- Network connectivity
- Trading rules initialization
- Balance updates
- Order creation/cancellation
- Fee calculations
- Trading pair symbol mapping
- Exchange status monitoring

**Run:** `pytest test_rkex_exchange.py -v`

### 5. `test_rkex_integration.py` ⭐ **Live API Tests**
Integration tests against the live API at `https://apiengine.demoapps.space`.

**Test Coverage:**
- Live /ping endpoint
- Live /time endpoint
- Live /exchangeinfo endpoint
- API key generation (with Bearer token)
- Connector initialization

**Run:** `pytest test_rkex_integration.py -v`

## Running All Tests

```bash
# Run all tests
pytest test/hummingbot/connector/exchange/rkex/ -v

# Run with coverage
pytest test/hummingbot/connector/exchange/rkex/ --cov=hummingbot.connector.exchange.rkex --cov-report=html

# Run specific test file
pytest test/hummingbot/connector/exchange/rkex/test_rkex_auth.py -v

# Run specific test
pytest test/hummingbot/connector/exchange/rkex/test_rkex_auth.py::RkexAuthTests::test_rest_auth_signature_get_request -v
```

## API Endpoints

### Live API Base URL
```
https://apiengine.demoapps.space
```

### Public Endpoints (No Auth Required)
- `GET /ping` - Health check
- `GET /time` - Server time
- `GET /exchangeinfo` - Trading pairs and rules
- `GET /depth` - Order book depth
- `GET /ticker/24hr` - 24h ticker stats
- `GET /ticker/bookTicker` - Best bid/ask
- `GET /ticker/price` - Latest prices

### Private Endpoints (Auth Required)
- `GET /balance` - Account balances
- `POST /order` - Create new order
- `GET /order/active-orders` - Get open orders
- `GET /order/all-orders` - Get all orders
- `DELETE /order/:id` - Cancel order
- `GET /myTrades` - Get trade history
- `POST /apikey/generate` - Generate API keys (requires Bearer token)
- `GET /apikey` - Get API keys
- `DELETE /apikey` - Delete API keys

## Authentication

### Two-Level Authentication

#### 1. Bearer Token (for API key generation)
Used for the `/apikey/generate` endpoint to create trading API keys.

```python
headers = {
    "Authorization": "Bearer YOUR_JWT_TOKEN"
}
```

#### 2. HMAC-SHA256 Signature (for trading operations)
Used for all trading endpoints (orders, balances, trades).

```python
headers = {
    "x-api-key": "YOUR_API_KEY",
    "x-timestamp": "1234567890000",  # milliseconds
    "x-signature": "hmac_sha256_signature"
}
```

**Signature Generation:**
```python
signature_payload = f"{timestamp}{params or data}"
signature = hmac.new(
    secret_key.encode("utf-8"),
    signature_payload.encode("utf-8"),
    digestmod="sha256"
).hexdigest()
```

## Test Environment Setup

### Prerequisites
```bash
pip install pytest pytest-asyncio aioresponses aiohttp
```

### Environment Variables (Optional)
```bash
export RKEX_API_KEY="your_api_key"
export RKEX_API_SECRET="your_api_secret"
export RKEX_BEARER_TOKEN="your_bearer_token"
```

## Notes

- Integration tests connect to the live API and may be skipped if the API is unreachable
- Bearer token for API key generation may expire - update in `test_rkex_integration.py` if needed
- All syntax checks pass with no errors
- Tests follow Hummingbot connector testing patterns (based on Bybit, Binance connectors)

## Test Statistics

- **Total Test Files:** 5
- **Total Test Cases:** 30+
- **Code Coverage:** Comprehensive (auth, utils, web utils, exchange, integration)
- **Live API Tests:** Yes (optional, can be skipped)

## Contributing

When adding new tests:
1. Follow existing test patterns
2. Use descriptive test names starting with `test_`
3. Add docstrings explaining what each test covers
4. Update this README with new test information
5. Ensure all tests pass before committing
