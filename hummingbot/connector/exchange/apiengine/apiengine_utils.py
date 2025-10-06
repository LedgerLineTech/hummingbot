from decimal import Decimal
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    # TODO: Adjust based on API response format
    return exchange_info.get("status", "TRADING") == "TRADING"


class ApiEngineConfigMap(BaseConnectorConfigMap):
    connector: str = "apiengine"
    apiengine_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your ApiEngine API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    apiengine_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your ApiEngine API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="apiengine")


KEYS = ApiEngineConfigMap.model_construct()

OTHER_DOMAINS = []
OTHER_DOMAINS_PARAMETER = {}
OTHER_DOMAINS_EXAMPLE_PAIR = {}
OTHER_DOMAINS_DEFAULT_FEES = {}
OTHER_DOMAINS_KEYS = {}