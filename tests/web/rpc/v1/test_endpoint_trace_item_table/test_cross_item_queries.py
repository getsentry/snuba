from datetime import datetime

import pytest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
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

from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
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


def create_trace_id_column() -> Column:
    """Helper to create a trace_id column."""
    return Column(
        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.trace_id"),
        label="trace_id",
    )


def create_trace_item_table_request(
    start_time: datetime,
    end_time: datetime,
    trace_item_type: TraceItemType.ValueType,
    columns: list[Column],
    trace_filters: list[TraceItemFilterWithType] | None = None,
    group_by: list[AttributeKey] | None = None,
    limit: int = 100,
) -> TraceItemTableRequest:
    """Helper to create TraceItemTableRequest with common defaults."""
    return TraceItemTableRequest(
        meta=create_request_meta(start_time, end_time, trace_item_type),
        columns=columns,
        trace_filters=trace_filters or [],
        group_by=group_by or [],
        limit=limit,
    )


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemTableCrossItemQueries(BaseApiTest):
    def test_cross_item_query_basic(self) -> None:
        """Test basic cross item query functionality."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # Create filters for cross item query
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

        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            columns=[create_trace_id_column()],
            trace_filters=trace_filters,
        )

        response = EndpointTraceItemTable().execute(message)

        # Should return spans from traces that match all filter conditions
        # Only the first 3 traces should match
        assert len(response.column_values) == 1
        assert len(response.column_values[0].results) == 3

    def test_cross_item_query_three_item_types(self) -> None:
        """Test cross item query with three different item types."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # Create filters for all three item types
        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            trace_filter(
                comparison_filter("error.attr3", "val3"),
                TraceItemType.TRACE_ITEM_TYPE_ERROR,
            ),
        ]

        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            columns=[create_trace_id_column()],
            trace_filters=trace_filters,
        )

        response = EndpointTraceItemTable().execute(message)

        # Should return logs from traces that match all three filter conditions
        assert len(response.column_values) == 1
        assert len(response.column_values[0].results) == 3

    def test_cross_item_query_no_matches(self) -> None:
        """Test cross item query when no traces match all conditions."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        # Create filters that won't match any traces
        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            trace_filter(
                comparison_filter("log.attr2", "other_val2"),
                TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
        ]

        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            columns=[create_trace_id_column()],
            trace_filters=trace_filters,
        )

        response = EndpointTraceItemTable().execute(message)

        # Should return no results
        assert len(response.column_values) == 0

    def test_cross_item_query_with_aggregation(self) -> None:
        """Test cross item query with aggregation functions."""
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

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

        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            columns=[
                Column(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_COUNT,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="sentry.trace_id"
                        ),
                        label="count",
                    )
                )
            ],
            trace_filters=trace_filters,
        )

        response = EndpointTraceItemTable().execute(message)

        assert len(response.column_values) == 1
        assert response.column_values[0].results[0].val_double == 3.0

    def test_cross_item_query_different_result_item_type(self) -> None:
        """Test cross item query returning different item type than filters."""
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

        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_ERROR,  # Return errors
            columns=[
                create_trace_id_column(),
                Column(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="error.attr3"),
                    label="error_attr3",
                ),
            ],
            trace_filters=trace_filters,
        )

        response = EndpointTraceItemTable().execute(message)

        assert len(response.column_values[0].results) == 3

        error_attr_values = [
            result.val_str for result in response.column_values[1].results
        ]
        assert all(val == "val3" for val in error_attr_values)
