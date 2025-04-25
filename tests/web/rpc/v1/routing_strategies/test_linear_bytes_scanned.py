from unittest.mock import MagicMock

import pytest

from snuba import state
from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.linear_bytes_scanned_storage_routing import (
    LinearBytesScannedRoutingStrategy,
)
from snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)


@pytest.mark.redis_db
@pytest.mark.parametrize(
    "most_downsampled_query_bytes_scanned, bytes_scanned_limit, expected_tier, expected_estimated_bytes_scanned",
    [
        (100, 99, Tier.TIER_64, 100),
        (100, 200, Tier.TIER_64, 100),
        (100, 900, Tier.TIER_8, 800),
        (100, 6500, Tier.TIER_1, 6400),
        (100, 51300, Tier.TIER_1, 6400),
    ],
)
def test_get_target_tier(
    most_downsampled_query_bytes_scanned: int,
    bytes_scanned_limit: int,
    expected_tier: Tier,
    expected_estimated_bytes_scanned: int,
) -> None:
    timer = MagicMock(spec=Timer)
    strategy = LinearBytesScannedRoutingStrategy()
    context = RoutingContext(MagicMock(), timer, MagicMock(), MagicMock())

    state.set_config(
        "LinearBytesScannedRoutingStrategy_bytes_scanned_per_query_limit",
        bytes_scanned_limit,
    )
    target_tier = strategy._get_target_tier(
        most_downsampled_tier_query_result=QueryResult(result={"profile": {"progress_bytes": most_downsampled_query_bytes_scanned}}, extra={}),  # type: ignore
        routing_context=context,
    )
    assert target_tier == expected_tier
    assert (
        context.extra_info["estimated_target_tier_bytes_scanned"]
        == expected_estimated_bytes_scanned
    )


@pytest.mark.redis_db
def test_target_tier_is_1_if_most_downsampled_query_bytes_scanned_is_0() -> None:
    timer = MagicMock(spec=Timer)
    strategy = LinearBytesScannedRoutingStrategy()
    context = RoutingContext(MagicMock(), timer, MagicMock(), MagicMock())

    state.set_config(
        "LinearBytesScannedRoutingStrategy_bytes_scanned_per_query_limit",
        10000,
    )
    target_tier = strategy._get_target_tier(
        most_downsampled_tier_query_result=QueryResult(result={"profile": {"progress_bytes": 0}}, extra={}),  # type: ignore
        routing_context=context,
    )
    assert target_tier == Tier.TIER_1
