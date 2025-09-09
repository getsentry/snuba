from datetime import datetime

import pytest
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    Expression,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import (
    TraceItemFilterWithType,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from tests.base import BaseApiTest
from tests.web.rpc.v1.test_utils import (
    comparison_filter,
    create_cross_item_test_data,
    create_request_meta,
    write_cross_item_data_to_storage,
)


def trace_filter(
    filter: TraceItemFilter, item_type: TraceItemType.ValueType
) -> TraceItemFilterWithType:
    """Helper to create trace filter with type."""
    return TraceItemFilterWithType(item_type=item_type, filter=filter)


def create_time_series_request(
    start_time: datetime,
    end_time: datetime,
    trace_item_type: TraceItemType.ValueType,
    expressions: list[Expression],
    trace_filters: list[TraceItemFilterWithType] | None = None,
    group_by: list[AttributeKey] | None = None,
    granularity_secs: int = 60,
) -> TimeSeriesRequest:
    """Helper to create TimeSeriesRequest with common defaults."""
    return TimeSeriesRequest(
        meta=create_request_meta(start_time, end_time, trace_item_type),
        expressions=expressions,
        trace_filters=trace_filters or [],
        group_by=group_by or [],
        granularity_secs=granularity_secs,
    )


def create_count_expression(label: str = "count") -> Expression:
    """Helper to create a count aggregation expression."""
    return Expression(
        aggregation=AttributeAggregation(
            aggregate=Function.FUNCTION_COUNT,
            label=label,
        ),
        label=label,
    )


def create_sum_expression(attribute_name: str, label: str) -> Expression:
    """Helper to create a sum aggregation expression."""
    return Expression(
        aggregation=AttributeAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name=attribute_name),
            label=label,
        ),
        label=label,
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesCrossItemQueries(BaseApiTest):
    def test_cross_item_filtering_reduces_time_series_data(self) -> None:
        """Test that cross-item filters correctly reduce the time series data returned."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # First, get baseline count without cross-item filters
        baseline_message = create_time_series_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            expressions=[create_count_expression()],
            trace_filters=[],  # No cross-item filters
            granularity_secs=3600,
        )

        baseline_response = EndpointTimeSeries().execute(baseline_message)
        baseline_count = sum(
            dp.data for dp in baseline_response.result_timeseries[0].data_points if dp.data_present
        )

        # Now apply cross-item filters that should reduce the data
        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        filtered_message = create_time_series_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            expressions=[create_count_expression()],
            trace_filters=trace_filters,
            granularity_secs=3600,
        )

        filtered_response = EndpointTimeSeries().execute(filtered_message)
        filtered_count = sum(
            dp.data for dp in filtered_response.result_timeseries[0].data_points if dp.data_present
        )

        # Cross-item filtering should reduce the count
        assert filtered_count < baseline_count
        assert filtered_count > 0  # But we should still have some data

    def test_cross_item_query_returns_different_item_type(self) -> None:
        """Test cross-item filtering works when querying different item type than filters."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # Filter on spans and logs, but return errors
        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        message = create_time_series_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_ERROR,  # Query for errors
            expressions=[create_count_expression()],
            trace_filters=trace_filters,
            granularity_secs=3600,
        )

        response = EndpointTimeSeries().execute(message)

        # Should return time series for errors from traces that match the span and log conditions
        assert len(response.result_timeseries) == 1
        timeseries = response.result_timeseries[0]
        assert timeseries.label == "count"
        assert len(timeseries.data_points) > 0

        # Should have some data (errors should exist in matching traces)
        total_count = sum(dp.data for dp in timeseries.data_points if dp.data_present)
        assert total_count > 0

    def test_cross_item_query_no_matching_traces(self) -> None:
        """Test cross-item query with conditions that match no traces returns zero data."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # Create filters that won't match any traces
        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "nonexistent_value"),  # This won't match
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        message = create_time_series_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            expressions=[create_count_expression()],
            trace_filters=trace_filters,
            granularity_secs=3600,
        )

        response = EndpointTimeSeries().execute(message)

        # Should return time series with zero data
        assert len(response.result_timeseries) == 1
        timeseries = response.result_timeseries[0]

        # All data points should have zero data
        total_count = sum(dp.data for dp in timeseries.data_points if dp.data_present)
        assert total_count == 0
