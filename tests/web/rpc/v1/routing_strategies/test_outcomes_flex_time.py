import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_relay.consts import DataCategory

from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.common.pagination import FlexibleTimeWindowPageWithFilters
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time import (
    OutcomesFlexTimeRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
    TimeWindow,
)
from tests.web.rpc.v1.routing_strategies.common import (
    override_component_config,
    store_outcomes_data,
)

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
    hours=24
)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(
    start: datetime,
    end: datetime,
    downsampled_storage_config: DownsampledStorageConfig | None = None,
) -> RequestMeta:
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
        downsampled_storage_config=downsampled_storage_config,
    )


def store_outcomes() -> None:
    outcome_data = []
    for hour in range(25):
        time = BASE_TIME - timedelta(hours=hour)
        outcome_data.append((time, 10_000_000))

    store_outcomes_data(outcome_data, DataCategory.LOG_ITEM, org_id=_ORG_ID, project_id=_PROJECT_ID)


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_flex_time_routing_strategy_with_data() -> None:
    # store 10 million log items every hour for 24 hours
    store_outcomes()
    strategy = OutcomesFlexTimeRoutingStrategy()
    # query the last 24 hours
    request = TraceItemTableRequest(
        meta=_get_request_meta(BASE_TIME - timedelta(hours=24), BASE_TIME)
    )
    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_LOG
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    with override_component_config(strategy, "max_items_to_query", 120_000_000):
        routing_decision = strategy.get_routing_decision(
            RoutingContext(
                in_msg=request,
                timer=Timer("test"),
                query_id=uuid.uuid4().hex,
            )
        )
    assert routing_decision.time_window is not None
    # time range should be 12 hours because 120M items / 10M items per hour = 12 hours
    assert (
        routing_decision.time_window.start_timestamp.seconds
        == (BASE_TIME - timedelta(hours=12)).timestamp()
    )
    assert routing_decision.time_window.end_timestamp.seconds == BASE_TIME.timestamp()


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_flex_time_routing_strategy_with_data_and_page_token() -> None:
    store_outcomes()
    strategy = OutcomesFlexTimeRoutingStrategy()
    # this is the case where the original request time range is being shortened by the page token
    # so even though the original request is for the past 24 hours, the page token specifies the request from 12 hours ago to 24 hours ago

    page_token_start_timestamp = BASE_TIME - timedelta(hours=24)
    page_token_end_timestamp = BASE_TIME - timedelta(hours=12)
    start_timestamp = Timestamp(seconds=int(page_token_start_timestamp.timestamp()))
    end_timestamp = Timestamp(seconds=int(page_token_end_timestamp.timestamp()))

    page_token = FlexibleTimeWindowPageWithFilters.create(
        TraceItemTableRequest(
            meta=_get_request_meta(BASE_TIME - timedelta(hours=24), BASE_TIME),
        ),
        TimeWindow(start_timestamp, end_timestamp),
        [],
    )
    request = TraceItemTableRequest(
        meta=_get_request_meta(BASE_TIME - timedelta(hours=24), BASE_TIME),
        page_token=page_token.page_token,
    )

    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_LOG
    request.meta.downsampled_storage_config.mode = (
        DownsampledStorageConfig.MODE_HIGHEST_ACCURACY_FLEXTIME
    )
    with override_component_config(strategy, "max_items_to_query", 120_000_000):
        routing_decision = strategy.get_routing_decision(
            RoutingContext(
                in_msg=request,
                timer=Timer("test"),
                query_id=uuid.uuid4().hex,
            )
        )
    assert routing_decision.time_window is not None
    assert (
        routing_decision.time_window.start_timestamp.seconds
        == page_token_start_timestamp.timestamp()
    )
    assert (
        routing_decision.time_window.end_timestamp.seconds == page_token_end_timestamp.timestamp()
    )


@pytest.mark.redis_db
def test_routing_query_is_exempt_from_allocation_policies() -> None:
    strategy = OutcomesFlexTimeRoutingStrategy()
    request = TraceItemTableRequest(
        meta=_get_request_meta(BASE_TIME - timedelta(hours=24), BASE_TIME)
    )
    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_LOG

    captured_requests = []

    def fake_run_query(dataset: Any, request: Any, timer: Any) -> Any:
        captured_requests.append(request)
        return QueryResult(
            result={"data": [{"num_items": 0}]},
            extra={"stats": {}, "sql": "", "experiments": {}},
        )

    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time.run_query",
        side_effect=fake_run_query,
    ):
        strategy.get_ingested_items_for_timerange(
            RoutingContext(
                in_msg=request,
                timer=Timer("test"),
                query_id=uuid.uuid4().hex,
            ),
            TimeWindow(
                start_timestamp=request.meta.start_timestamp,
                end_timestamp=request.meta.end_timestamp,
            ),
        )

    assert len(captured_requests) == 1
    tenant_ids = captured_requests[0].attribution_info.tenant_ids
    assert tenant_ids["cross_org_query"] == 1


@pytest.mark.redis_db
def test_empty_outcomes_result_counts_as_zero_items() -> None:
    strategy = OutcomesFlexTimeRoutingStrategy()
    request = TraceItemTableRequest(
        meta=_get_request_meta(BASE_TIME - timedelta(hours=24), BASE_TIME)
    )
    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_LOG

    with mock.patch(
        "snuba.web.rpc.storage_routing.routing_strategies.outcomes_flex_time.run_query",
        return_value=QueryResult(
            result={"data": []},
            extra={"stats": {}, "sql": "", "experiments": {}},
        ),
    ):
        ingested_items = strategy.get_ingested_items_for_timerange(
            RoutingContext(
                in_msg=request,
                timer=Timer("test"),
                query_id=uuid.uuid4().hex,
            ),
            TimeWindow(
                start_timestamp=request.meta.start_timestamp,
                end_timestamp=request.meta.end_timestamp,
            ),
        )

    assert ingested_items == 0
