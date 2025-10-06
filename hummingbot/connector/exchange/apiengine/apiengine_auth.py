from typing import Any, Dict

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class ApiEngineAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the Bearer token to the request header for authenticated interactions.
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication())
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. ApiEngine does not use this
        functionality
        """
        return request  # pass-through

    def header_for_authentication(self) -> Dict[str, str]:
        return {
            "x-api-key": self.api_key,
            "x-api-secret": self.secret_key,
            "Authorization": f"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkNGUzNDhhMi1mMDU3LTQ3MjgtYWZlYi05ODBjZTY4ZTgzMmQiLCJlbWFpbCI6InJpc2hpQHRlc3RtYWlsLmNvbSIsInJvbGVzIjpbIlVTRVIiLCJBRE1JTiJdLCJpYXQiOjE3NTkyOTYwNDEsImV4cCI6MTc1OTMzMjA0MX0.fukEW3KvHiZmk8q7hmTCS60vYwuuNDJ5tpB2V6N9LaU"
        }