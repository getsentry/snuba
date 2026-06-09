import uuid
from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
    hours=24
)
_PROJECT_ID = 1
_ORG_ID = 1
_OTHER_ORG_ID = 9999


def _get_request_meta(organization_id: int = _ORG_ID) -> RequestMeta:
    start = BASE_TIME - timedelta(hours=24)
    end = BASE_TIME
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=organization_id,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
    )


def _build_context(organization_id: int = _ORG_ID) -> RoutingContext:
    request = TraceItemTableRequest(meta=_get_request_meta(organization_id=organization_id))
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    return RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )


@pytest.mark.eap
@pytest.mark.redis_db
def test_no_override_uses_allocation_policy_value() -> None:
    strategy = OutcomesBasedRoutingStrategy()
    routing_decision = strategy.get_routing_decision(_build_context())
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert "org_clickhouse_setting_overrides" not in routing_decision.routing_context.extra_info


@pytest.mark.eap
@pytest.mark.redis_db
def test_org_override_replaces_allocation_policy_value() -> None:
    strategy = OutcomesBasedRoutingStrategy()
    strategy.set_config_value("organization_max_threads_override", 4, {"organization_id": _ORG_ID})
    routing_decision = strategy.get_routing_decision(_build_context())
    assert routing_decision.clickhouse_settings["max_threads"] == 4
    assert routing_decision.routing_context.extra_info["org_clickhouse_setting_overrides"] == {
        "max_threads": 4
    }


@pytest.mark.eap
@pytest.mark.redis_db
def test_org_override_can_raise_above_allocation_policy_value() -> None:
    strategy = OutcomesBasedRoutingStrategy()
    # Baseline policy value is 10 (asserted in the no-override test); set the
    # override higher to confirm absolute precedence — not min/cap semantics.
    strategy.set_config_value("organization_max_threads_override", 32, {"organization_id": _ORG_ID})
    routing_decision = strategy.get_routing_decision(_build_context())
    assert routing_decision.clickhouse_settings["max_threads"] == 32


@pytest.mark.eap
@pytest.mark.redis_db
def test_override_scoped_to_organization_id() -> None:
    strategy = OutcomesBasedRoutingStrategy()
    strategy.set_config_value("organization_max_threads_override", 4, {"organization_id": _ORG_ID})

    # Same strategy instance, a different org_id should not be affected.
    other_decision = strategy.get_routing_decision(_build_context(organization_id=_OTHER_ORG_ID))
    assert other_decision.clickhouse_settings == {"max_threads": 10}
    assert "org_clickhouse_setting_overrides" not in other_decision.routing_context.extra_info

    # And the configured org still sees the override.
    target_decision = strategy.get_routing_decision(_build_context())
    assert target_decision.clickhouse_settings["max_threads"] == 4
