from unittest.mock import MagicMock, patch

import pytest


from snuba import state
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategies.linear_bytes_scanned_storage_routing import LinearBytesScannedRoutingStrategy
from snuba.web.rpc.v1.resolvers.R_eap_items.routing_strategy_selector import _DEFAULT_STORAGE_ROUTING_CONFIG, _STORAGE_ROUTING_CONFIG_KEY, RoutingStrategySelector
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing import BaseRoutingStrategy

class ToyRoutingStrategy1(BaseRoutingStrategy):
    pass

class ToyRoutingStrategy2(BaseRoutingStrategy):
    pass

@pytest.mark.redis_db
def test_strategy_selector_selects_linear_bytes_scanned_strategy_if_fails_to_parse_config() -> None:
    state.set_config(_STORAGE_ROUTING_CONFIG_KEY, "{}")
    storage_routing_config = RoutingStrategySelector().get_storage_routing_strategy_config()
    assert storage_routing_config == _DEFAULT_STORAGE_ROUTING_CONFIG

@pytest.mark.redis_db
def test_valid_config_is_parsed_correctly() -> None:
    state.set_config(_STORAGE_ROUTING_CONFIG_KEY, '{"version": 1, "LinearBytesScannedRoutingStrategy": 0.1, "ToyRoutingStrategy1": 0.2, "ToyRoutingStrategy2": 0.70}')
    storage_routing_config = RoutingStrategySelector().get_storage_routing_strategy_config()
    print(storage_routing_config)
    assert storage_routing_config.version == 1
    assert storage_routing_config.routing_strategy_and_percentage_routed[LinearBytesScannedRoutingStrategy] == 0.1
    assert storage_routing_config.routing_strategy_and_percentage_routed[ToyRoutingStrategy1] == 0.2
    assert storage_routing_config.routing_strategy_and_percentage_routed[ToyRoutingStrategy2] == 0.7
