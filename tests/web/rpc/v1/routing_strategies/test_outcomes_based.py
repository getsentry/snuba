import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_get_traces_pb2 import GetTracesRequest
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    TraceItemFilterWithType,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba import state
from snuba.downsampled_storage_tiers import Tier
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.storage_routing.common import extract_message_meta
from snuba.web.rpc.storage_routing.routing_strategies.outcomes_based import (
    OutcomesBasedRoutingStrategy,
)
from snuba.web.rpc.storage_routing.routing_strategies.storage_routing import (
    RoutingContext,
)
from tests.web.rpc.v1.routing_strategies.common import store_outcomes_data

BASE_TIME = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
    hours=24
)
_PROJECT_ID = 1
_ORG_ID = 1


def _get_request_meta(
    start: datetime | None = None,
    end: datetime | None = None,
    hour_interval: int | None = None,
    downsampled_storage_config: DownsampledStorageConfig | None = None,
    trace_item_type: TraceItemType.ValueType | None = None,
) -> RequestMeta:
    hour_interval = hour_interval or 24
    start = start or BASE_TIME - timedelta(hours=hour_interval)
    end = end or BASE_TIME
    trace_item_type = trace_item_type or TraceItemType.TRACE_ITEM_TYPE_SPAN
    return RequestMeta(
        project_ids=[_PROJECT_ID],
        organization_id=_ORG_ID,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(start.timestamp())),
        end_timestamp=Timestamp(seconds=int(end.timestamp())),
        trace_item_type=trace_item_type,
        downsampled_storage_config=downsampled_storage_config,
    )


@pytest.fixture
def store_outcomes_fixture(eap: Any) -> None:
    # Generate 24 hours of outcomes data with 1M outcomes per hour
    outcome_data = []
    for hour in range(24):
        time = BASE_TIME - timedelta(hours=hour)
        outcome_data.append((time, 1_000_000))

    store_outcomes_data(outcome_data)


@pytest.mark.redis_db
@pytest.mark.eap
def test_outcomes_based_routing_queries_daily_table() -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta(hour_interval=2400))  # 100 days
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )
    in_msg_meta = extract_message_meta(context.in_msg)
    assert strategy._use_daily(in_msg_meta=in_msg_meta)

    routing_decision = strategy.get_routing_decision(context)

    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_item_type_full_retention() -> None:
    """
    Certain item types will not use the long term retention downsampling,
    find them in ITEM_TYPE_FULL_RETENTION routing_strategies/common.py
    """
    state.set_config(
        "enable_long_term_retention_downsampling",
        1,
    )
    strategy = OutcomesBasedRoutingStrategy()

    # request that queries last 50 days of data
    end_time = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1200)  # 50 days
    request = TraceItemTableRequest(
        meta=_get_request_meta(
            start=start_time,
            end=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_RESULT,
        )
    )
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )
    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_item_type_full_retention_preprod() -> None:
    """
    PREPROD item type should not use long term retention downsampling,
    it should always fetch tier1 for its 90 day retention period.
    """
    state.set_config(
        "enable_long_term_retention_downsampling",
        1,
    )
    strategy = OutcomesBasedRoutingStrategy()

    # request that queries last 50 days of data
    end_time = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1200)  # 50 days
    request = TraceItemTableRequest(
        meta=_get_request_meta(
            start=start_time,
            end=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_PREPROD,
        )
    )
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )
    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_based_routing_sampled_data_past_thirty_days() -> None:
    state.set_config(
        "enable_long_term_retention_downsampling",
        1,
    )
    strategy = OutcomesBasedRoutingStrategy()
    end_time = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    start_time = end_time - timedelta(hours=720)  # 30 days
    request = TraceItemTableRequest(meta=_get_request_meta(start=start_time, end=end_time))
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run

    # request that queries last 50 days of data
    start_time = end_time - timedelta(hours=1200)  # 50 days
    request = TraceItemTableRequest(meta=_get_request_meta(start=start_time, end=end_time))
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run

    # request(s) that query window of 30 minutes, but with timestamps 40 days ago
    # one in MODE_NORMAL, one in MODE_HIGHEST_ACCURACY (which is ignored in favor of
    # the enable_long_term_retention_downsampling)
    start = datetime.now(tz=UTC) - timedelta(days=40, minutes=30)
    end = datetime.now(tz=UTC) - timedelta(days=40)

    # normal
    request = TraceItemTableRequest(meta=_get_request_meta(start=start, end=end))
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run

    # highest accuracy
    request = TraceItemTableRequest(meta=_get_request_meta(start=start, end=end))
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    routing_decision = strategy.get_routing_decision(context)
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_based_routing_normal_mode(store_outcomes_fixture: Any) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_based_routing_downsample(store_outcomes_fixture: Any) -> None:
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 5_000_000)
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_NORMAL

    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )
    assert routing_decision.tier == Tier.TIER_8
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 500_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )
    assert routing_decision.tier == Tier.TIER_64
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run

    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 50_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )
    assert routing_decision.tier == Tier.TIER_512
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_based_routing_highest_accuracy_mode(store_outcomes_fixture: Any) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.downsampled_storage_config.mode = DownsampledStorageConfig.MODE_HIGHEST_ACCURACY
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )

    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


@pytest.mark.eap
@pytest.mark.redis_db
def test_outcomes_based_routing_defaults_to_tier1_for_unspecified_item_type(
    store_outcomes_fixture: Any,
) -> None:
    strategy = OutcomesBasedRoutingStrategy()

    request = TraceItemTableRequest(meta=_get_request_meta())
    request.meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED
    state.set_config("OutcomesBasedRoutingStrategy.max_items_before_downsampling", 50_000)
    routing_decision = strategy.get_routing_decision(
        RoutingContext(
            in_msg=request,
            timer=Timer("test"),
            query_id=uuid.uuid4().hex,
        )
    )
    assert routing_decision.tier == Tier.TIER_1
    assert routing_decision.clickhouse_settings == {"max_threads": 10}
    assert routing_decision.can_run


def test_get_item_types_in_query_trace_item_table_request_with_filters() -> None:
    """Test extracting item types from TraceItemTableRequest with multiple trace_filters."""
    strategy = OutcomesBasedRoutingStrategy()
    request_meta = _get_request_meta()

    trace_filter_span = TraceItemFilterWithType(
        item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        filter=TraceItemFilter(),
    )
    trace_filter_log = TraceItemFilterWithType(
        item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
        filter=TraceItemFilter(),
    )
    request = TraceItemTableRequest(
        meta=request_meta,
        trace_filters=[trace_filter_span, trace_filter_log],
    )
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    item_types = strategy.get_item_types_in_query(context)

    assert set(item_types) == {
        TraceItemType.TRACE_ITEM_TYPE_SPAN,
        TraceItemType.TRACE_ITEM_TYPE_LOG,
    }


def test_get_item_types_in_query_time_series_request_with_filters() -> None:
    """Test extracting item types from TimeSeriesRequest with trace_filters."""
    strategy = OutcomesBasedRoutingStrategy()
    request_meta = _get_request_meta()

    trace_filter_span = TraceItemFilterWithType(
        item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        filter=TraceItemFilter(),
    )
    request = TimeSeriesRequest(
        meta=request_meta,
        trace_filters=[trace_filter_span],
    )
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    item_types = strategy.get_item_types_in_query(context)

    assert item_types == [TraceItemType.TRACE_ITEM_TYPE_SPAN]


def test_get_item_types_in_query_get_traces_request_with_filters() -> None:
    """Test extracting item types from GetTracesRequest with filters."""
    strategy = OutcomesBasedRoutingStrategy()
    request_meta = _get_request_meta()

    get_traces_filter = GetTracesRequest.TraceFilter(
        item_type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
        filter=TraceItemFilter(),
    )
    request = GetTracesRequest(
        meta=request_meta,
        filters=[get_traces_filter],
    )
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    item_types = strategy.get_item_types_in_query(context)

    assert item_types == [TraceItemType.TRACE_ITEM_TYPE_METRIC]


def test_get_item_types_in_query_fallback_to_meta() -> None:
    """Test fallback to meta.trace_item_type when no filters are present."""
    strategy = OutcomesBasedRoutingStrategy()
    request_meta = _get_request_meta()

    request = TraceItemTableRequest(meta=request_meta)
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    item_types = strategy.get_item_types_in_query(context)

    assert item_types == [TraceItemType.TRACE_ITEM_TYPE_SPAN]


def test_get_item_types_in_query_empty_when_unspecified() -> None:
    """Test returns empty list when no filters and trace_item_type is UNSPECIFIED."""
    strategy = OutcomesBasedRoutingStrategy()
    request_meta = _get_request_meta()
    request_meta.trace_item_type = TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    request = TraceItemTableRequest(meta=request_meta)
    context = RoutingContext(
        in_msg=request,
        timer=Timer("test"),
        query_id=uuid.uuid4().hex,
    )

    item_types = strategy.get_item_types_in_query(context)

    assert item_types == []
