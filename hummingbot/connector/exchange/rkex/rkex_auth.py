import hmac
from typing import Any, Dict
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class RkexAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication headers to the request for authenticated interactions.
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication(request))
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Rkex does not use this
        functionality
        """
        return request  # pass-through

    def header_for_authentication(self, request: RESTRequest = None) -> Dict[str, str]:
        """
        Generates authentication headers using API key and secret.
        If the API uses HMAC signature, it will be generated here.
        Otherwise, it uses simple API key/secret header authentication.
        """
        headers = {
            "x-api-key": self.api_key,
        }

        # Generate signature if request is provided
        if request is not None:
            timestamp = str(int(self.time_provider.time() * 1000))
            headers["x-timestamp"] = timestamp

            # Create signature string based on request method
            if request.method == RESTMethod.POST and request.data:
                signature_payload = f"{timestamp}{request.data}"
            elif request.params:
                signature_payload = f"{timestamp}{urlencode(request.params)}"
            else:
                signature_payload = timestamp

            # Generate HMAC signature
            signature = hmac.new(
                self.secret_key.encode("utf-8"),
                signature_payload.encode("utf-8"),
                digestmod="sha256"
            ).hexdigest()

            headers["x-signature"] = signature

        return headers