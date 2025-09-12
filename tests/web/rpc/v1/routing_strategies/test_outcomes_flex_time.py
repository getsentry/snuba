from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time import (
    OutcomesFlexTimeRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
    hours=24
)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(
    start: datetime | None = None,
    end: datetime | None = None,
    downsampled_storage_config: DownsampledStorageConfig | None = None,
) -> RequestMeta:
    start = start or BASE_TIME - timedelta(hours=1)  # Short time window for test
    end = end or BASE_TIME
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        downsampled_storage_config=downsampled_storage_config,
    )


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_flex_time_routing_strategy_basic() -> None:
    """Basic test that just exercises the routing strategy without complex assertions."""
    strategy = OutcomesFlexTimeRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )

    # Just basic assertions to make sure the strategy returns something reasonable
    assert routing_decision is not None
    assert routing_decision.strategy == strategy
    assert routing_decision.can_run is not None


def test_outcomes_flex_time_routing_strategy_highest_accuracy() -> None:
    """Test that highest accuracy mode works."""
    strategy = OutcomesFlexTimeRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_HIGHEST_ACCURACY

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
        )
    )

    # Basic assertions
    assert routing_decision is not None
    assert routing_decision.strategy == strategy
    assert routing_decision.can_run is not None
