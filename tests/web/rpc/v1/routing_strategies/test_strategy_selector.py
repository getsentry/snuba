import random

import pytest
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import state
from snuba.query.query_settings import HTTPQuerySettings
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.v1.resolvers.R_eap_items.resolver_time_series import build_query
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.linear_bytes_scanned_storage_routing import (
    LinearBytesScannedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategy_selector import (
    _DEFAULT_STORAGE_ROUTING_CONFIG,
    _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
    _STORAGE_ROUTING_CONFIG_OVERRIDE_KEY,
    RoutingStrategySelector,
    StorageRoutingConfig,
)


class ToyRoutingStrategy1(BaseRoutingStrategy):
    pass


class ToyRoutingStrategy2(BaseRoutingStrategy):
    pass


class ToyRoutingStrategy3(BaseRoutingStrategy):
    pass


@pytest.mark.redis_db
def test_strategy_selector_selects_default_if_no_config() -> None:
    storage_routing_config = RoutingStrategySelector().get_storage_routing_config(1)
    assert storage_routing_config == _DEFAULT_STORAGE_ROUTING_CONFIG


@pytest.mark.redis_db
def test_strategy_selector_selects_default_if_strategy_does_not_exist() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"NonExistentStrategy": 1}}',
    )
    storage_routing_config = RoutingStrategySelector().get_storage_routing_config(1)
    assert storage_routing_config == _DEFAULT_STORAGE_ROUTING_CONFIG


@pytest.mark.redis_db
def test_strategy_selector_selects_default_if_percentages_do_not_add_up() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"LinearBytesScannedRoutingStrategy": 0.1, "ToyRoutingStrategy1": 0.2, "ToyRoutingStrategy2": 0.10}}',
    )
    storage_routing_config = RoutingStrategySelector().get_storage_routing_config(1)
    assert storage_routing_config == _DEFAULT_STORAGE_ROUTING_CONFIG


@pytest.mark.redis_db
def test_valid_config_is_parsed_correctly() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"LinearBytesScannedRoutingStrategy": 0.1, "ToyRoutingStrategy1": 0.2, "ToyRoutingStrategy2": 0.70}}',
    )
    storage_routing_config = RoutingStrategySelector().get_storage_routing_config(1)
    assert storage_routing_config.version == 1
    assert storage_routing_config.get_routing_strategy_and_percentage_routed() == [
        ("LinearBytesScannedRoutingStrategy", 0.1),
        ("ToyRoutingStrategy1", 0.2),
        ("ToyRoutingStrategy2", 0.7),
    ]


@pytest.mark.redis_db
def test_selects_same_strategy_for_same_org_and_project_ids() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"LinearBytesScannedRoutingStrategy": 0.25, "ToyRoutingStrategy1": 0.25, "ToyRoutingStrategy2": 0.25, "ToyRoutingStrategy3": 0.25}}',
    )

    routing_context = RoutingContext(
        in_msg=TimeSeriesRequest(
            meta=RequestMeta(
                organization_id=11,
                project_ids=[14, 15, 16],
            ),
        ),
        timer=Timer(name="doesntmatter"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
    )

    for _ in range(50):
        assert isinstance(
            RoutingStrategySelector().select_routing_strategy(routing_context),
            LinearBytesScannedRoutingStrategy,
        )


@pytest.mark.redis_db
def test_selects_strategy_based_on_non_uniform_distribution() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"LinearBytesScannedRoutingStrategy": 0.10, "ToyRoutingStrategy1": 0.90}}',
    )

    strategy_counts = {LinearBytesScannedRoutingStrategy: 0, ToyRoutingStrategy1: 0}

    selector = RoutingStrategySelector()

    for _ in range(1000):
        routing_context = RoutingContext(
            in_msg=TimeSeriesRequest(
                meta=RequestMeta(
                    organization_id=random.randint(1, 1000),
                    project_ids=[
                        random.randint(1, 1000)
                        for _ in range(random.randint(1, random.randint(1, 10)))
                    ],
                ),
            ),
            timer=Timer(name="doesntmatter"),
            build_query=build_query,  # type: ignore
            query_settings=HTTPQuerySettings(),
        )
        strategy = selector.select_routing_strategy(routing_context)
        strategy_counts[type(strategy)] += 1

    # about 100 should be routed, 400 is a generous upper bound
    assert strategy_counts[LinearBytesScannedRoutingStrategy] < 400
    # about 900 should be routed, 600 is a generous lower bound
    assert strategy_counts[ToyRoutingStrategy1] > 600


@pytest.mark.redis_db
def test_config_ordering_does_not_affect_routing_consistency() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"ToyRoutingStrategy1": 0.25, "ToyRoutingStrategy2": 0.55, "LinearBytesScannedRoutingStrategy": 0.2}}',
    )

    routing_context = RoutingContext(
        in_msg=TimeSeriesRequest(
            meta=RequestMeta(
                organization_id=10,
                project_ids=[11, 12],
            ),
        ),
        timer=Timer(name="doesntmatter"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
    )

    assert isinstance(
        RoutingStrategySelector().select_routing_strategy(routing_context),
        ToyRoutingStrategy1,
    )

    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"ToyRoutingStrategy1": 0.25, "LinearBytesScannedRoutingStrategy": 0.2, "ToyRoutingStrategy2": 0.55}}',
    )

    assert isinstance(
        RoutingStrategySelector().select_routing_strategy(routing_context),
        ToyRoutingStrategy1,
    )


@pytest.mark.redis_db
def test_selects_override_if_it_exists() -> None:
    state.set_config(
        _DEFAULT_STORAGE_ROUTING_CONFIG_KEY,
        '{"version": 1, "config": {"LinearBytesScannedRoutingStrategy": 0.25, "ToyRoutingStrategy1": 0.25, "ToyRoutingStrategy2": 0.25, "ToyRoutingStrategy3": 0.25}}',
    )

    state.set_config(
        _STORAGE_ROUTING_CONFIG_OVERRIDE_KEY,
        '{"10": {"version": 1, "config": {"ToyRoutingStrategy1": 0.95, "ToyRoutingStrategy2": 0.05}}}',
    )

    routing_context = RoutingContext(
        in_msg=TimeSeriesRequest(
            meta=RequestMeta(
                organization_id=10,
                project_ids=[11, 12],
            ),
        ),
        timer=Timer(name="doesntmatter"),
        build_query=build_query,  # type: ignore
        query_settings=HTTPQuerySettings(),
    )

    assert RoutingStrategySelector().get_storage_routing_config(
        routing_context.in_msg.meta.organization_id
    ) == StorageRoutingConfig(
        version=1,
        _routing_strategy_and_percentage_routed={
            "ToyRoutingStrategy1": 0.95,
            "ToyRoutingStrategy2": 0.05,
        },
    )
