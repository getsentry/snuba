from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from tests.web.rpc.v1.routing_strategies.common import override_component_config


def test_routing_strategy_reads_option() -> None:
    """Configs are read from the ``configurable_component_overrides`` sentry-option
    (then the code default)."""
    strategy = OutcomesBasedRoutingStrategy()

    assert strategy.get_config_value("some_default_config") == 100

    with override_component_config(strategy, "some_default_config", 7):
        assert strategy.get_config_value("some_default_config") == 7

    assert strategy.get_config_value("some_default_config") == 100
