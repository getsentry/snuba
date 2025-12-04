import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.configs.configuration import Configuration, ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.allocation_policies import (
    MAX_THRESHOLD,
    NO_SUGGESTION,
    NO_UNITS,
    AllocationPolicy,
    QueryResultOrError,
    QuotaAllowance,
)
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.load_retriever import LoadInfo
from snuba.web.rpc.storage_routing.routing_strategies.load_based_outcomes import (
    LoadBasedOutcomesRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    BaseRoutingStrategy,
    RoutingContext,
)

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(hour_interval: int = 1) -> RequestMeta:
    start = BASE_TIME - timedelta(hours=hour_interval)
    end = BASE_TIME
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        downsampled_storage_config=DownsampledStorageConfig(
            mode=DownsampledStorageConfig.MODE_NORMAL
        ),
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_load_based_routing_pass_through_even_if_policies_reject() -> None:
    class RejectAllPolicy(AllocationPolicy):
        def _additional_config_definitions(self) -> list[Configuration]:
            return []

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            return QuotaAllowance(
                can_run=False,
                max_threads=0,
                explanation={"reason": "reject all"},
                is_throttled=True,
                throttle_threshold=MAX_THRESHOLD,
                rejection_threshold=MAX_THRESHOLD,
                quota_used=0,
                quota_unit=NO_UNITS,
                suggestion=NO_SUGGESTION,
            )

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            return

    strategy = LoadBasedOutcomesRoutingStrategy()
    request = TraceItemTableRequest(meta=_get_request_meta(hour_interval=1))
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    with patch.object(
        BaseRoutingStrategy,
        "get_allocation_policies",
        return_value=[
            RejectAllPolicy(ResourceIdentifier(StorageKey("doesntmatter")), ["org_id"], {})
        ],
    ):
        with patch(
            "snuba.web.rpc.storage_routing.routing_strategies.storage_routing.get_cluster_loadinfo",
            return_value=LoadInfo(cluster_load=5.0, concurrent_queries=1),
        ):
            routing_decision = strategy.get_routing_decision(context)

    assert routing_decision.can_run is True
    assert routing_decision.clickhouse_settings.get("max_threads") == 10
    assert "load_based_pass_through" in routing_decision.routing_context.extra_info
    assert routing_decision.routing_context.cluster_load_info is not None
    assert routing_decision.routing_context.cluster_load_info.cluster_load == 5.0
