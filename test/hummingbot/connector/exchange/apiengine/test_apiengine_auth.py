import asyncio
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.connector.exchange.apiengine.apiengine_auth import ApiEngineAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class ApiEngineAuthTests(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.api_key = "testApiKey"
        self.secret_key = "testSecretKey"

        self.mock_time_provider = MagicMock()
        self.mock_time_provider.time.return_value = 1000.0

        self.auth = ApiEngineAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self.mock_time_provider,
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_auth_signature_get_request(self):
        params = {"symbol": "BTCUSDT", "limit": "100"}
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            is_auth_required=True,
            params=params,
            throttler_limit_id="/api/endpoint"
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(request.headers["x-api-key"], self.api_key)
        self.assertIsNotNone(request.headers["x-timestamp"])
        self.assertIsNotNone(request.headers["x-signature"])

        # Verify timestamp is milliseconds
        timestamp = int(request.headers["x-timestamp"])
        self.assertEqual(timestamp, 1000000)  # 1000.0 * 1000

    def test_rest_auth_signature_post_request(self):
        data = {"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.1"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://test.url/api/order",
            data=data,
            is_auth_required=True,
            throttler_limit_id="/api/order"
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(request.headers["x-api-key"], self.api_key)
        self.assertIsNotNone(request.headers["x-timestamp"])
        self.assertIsNotNone(request.headers["x-signature"])

    def test_add_auth_params_to_get_request_without_params(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            is_auth_required=True,
            throttler_limit_id="/api/endpoint"
        )
        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(request.headers["x-api-key"], self.api_key)
        self.assertIsNone(request.params)
        self.assertIsNone(request.data)
        self.assertIsNotNone(request.headers["x-signature"])

    def test_add_auth_params_to_get_request_with_params(self):
        params = {"param_z": "value_param_z", "param_a": "value_param_a"}
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            params=params,
            is_auth_required=True,
            throttler_limit_id="/api/endpoint"
        )

        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(len(request.params), 2)
        self.assertEqual(request.params["param_z"], "value_param_z")
        self.assertEqual(request.params["param_a"], "value_param_a")
        self.assertEqual(request.headers["x-api-key"], self.api_key)

    def test_add_auth_params_to_post_request(self):
        data = {"param_z": "value_param_z", "param_a": "value_param_a"}
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://apiengine-mock/api/endpoint",
            data=data,
            is_auth_required=True,
            throttler_limit_id="/api/endpoint"
        )

        self.async_run_with_timeout(self.auth.rest_authenticate(request))

        self.assertEqual(request.data["param_z"], "value_param_z")
        self.assertEqual(request.data["param_a"], "value_param_a")
        self.assertEqual(request.headers["x-api-key"], self.api_key)

    def test_ws_auth_passthrough(self):
        """Test that WebSocket authentication is a passthrough (not used)"""
        request = WSJSONRequest(payload={}, is_auth_required=True)
        ws_result = self.async_run_with_timeout(self.auth.ws_authenticate(request))

        # Should return the request as-is (passthrough)
        self.assertEqual(ws_result, request)

    def test_header_for_authentication_without_request(self):
        """Test header generation without request (simple API key only)"""
        headers = self.auth.header_for_authentication(None)

        self.assertEqual(headers["x-api-key"], self.api_key)
        self.assertNotIn("x-signature", headers)
        self.assertNotIn("x-timestamp", headers)

    def test_signature_generation_consistency(self):
        """Test that same request generates same signature"""
        params = {"symbol": "BTCUSDT"}
        request1 = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            params=params,
            is_auth_required=True,
            throttler_limit_id="/api/endpoint"
        )
        request2 = RESTRequest(
            method=RESTMethod.GET,
            url="https://test.url/api/endpoint",
            params=params.copy(),
            is_auth_required=True,
            throttler_limit_id="/api/endpoint"
        )

        self.async_run_with_timeout(self.auth.rest_authenticate(request1))
        self.async_run_with_timeout(self.auth.rest_authenticate(request2))

        # Both should have same signature with same timestamp
        self.assertEqual(request1.headers["x-signature"], request2.headers["x-signature"])
        self.assertEqual(request1.headers["x-timestamp"], request2.headers["x-timestamp"])
