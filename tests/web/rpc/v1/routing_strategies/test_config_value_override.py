import pytest

from snuba import state
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from tests.web.rpc.v1.routing_strategies.common import override_component_config


@pytest.mark.redis_db
def test_routing_strategy_reads_option_not_redis() -> None:
    """Routing strategies read config from the ``configurable_component_overrides``
    sentry-option (then the code default). The legacy Redis runtime config is not
    consulted."""
    strategy = OutcomesBasedRoutingStrategy()

    # A value written straight to the legacy Redis runtime config is ignored -- the
    # code default wins. (set_config_value is not used here because in tests it also
    # mirrors the write into the sentry-option that get_config_value reads.)
    state.set_config(
        strategy._build_config_key("some_default_config", {}),
        5,
        config_key=strategy._get_hash(),
    )
    assert strategy.get_config_value("some_default_config") == 100

    # The sentry-option is honored, cast to the config's declared int type.
    with override_component_config(strategy, "some_default_config", 7):
        assert strategy.get_config_value("some_default_config") == 7

    # Once the option entry is gone we return the default -- never the Redis value.
    assert strategy.get_config_value("some_default_config") == 100
