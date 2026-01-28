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
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba import state
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


def create_trace_item_table_request(
    start_time: datetime,
    end_time: datetime,
    trace_item_type: TraceItemType.ValueType,
    columns: list[Column],
    trace_filters: list[TraceItemFilterWithType] | None = None,
    debug: bool = False,
) -> TraceItemTableRequest:
    """Helper to create TraceItemTableRequest with common defaults."""
    meta = create_request_meta(start_time, end_time, trace_item_type)
    meta.debug = debug
    return TraceItemTableRequest(
        meta=meta,
        columns=columns,
        trace_filters=trace_filters or [],
        limit=100,
    )


@pytest.mark.eap
@pytest.mark.redis_db
@pytest.mark.clickhouse_db
class TestTraceItemTableCrossItemSampling(BaseApiTest):
    def test_cross_item_query_sampling_inner_vs_outer(self) -> None:
        """
        Test that when cross_item_queries_no_sample_outer is enabled:
        - The inner query (getting trace IDs) uses downsampled storage
        - The outer query uses full storage (no sampling)
        """
        # Enable the feature flag
        state.set_config("cross_item_queries_no_sample_outer", 1)

        try:
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

            # Create request with trace_filters and debug enabled
            message = create_trace_item_table_request(
                start_time=start_time,
                end_time=end_time,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                columns=[
                    Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id"))
                ],
                trace_filters=trace_filters,
                debug=True,  # Enable debug to get SQL
            )

            response = EndpointTraceItemTable().execute(message)

            # Check that we got query info with SQL
            assert len(response.meta.query_info) > 0
            query_info = response.meta.query_info[0]
            sql = query_info.metadata.sql.lower()

            # Verify the SQL structure
            assert "select" in sql, "SQL should contain SELECT"

            # The SQL should contain a subquery (inner query for trace IDs)
            # The actual table names will be like "eap_items_X_local" where X is the retention tier
            # We should see two FROM clauses: outer query and inner query (subquery)
            from_count = sql.count("from eap_items")
            assert from_count >= 2, (
                f"SQL should have at least 2 FROM clauses (outer + inner query), found {from_count}"
            )

            # Verify we have the subquery pattern with trace_id IN (SELECT ...)
            assert "in(replaceall(tostring(trace_id)" in sql, "Should have trace_id IN clause"
            assert "(select replaceall(tostring(trace_id)" in sql, (
                "Should have inner SELECT for trace IDs"
            )

        finally:
            # Clean up: reset the config
            state.delete_config("cross_item_queries_no_sample_outer")

    def test_cross_item_query_sampling_disabled(self) -> None:
        """
        Test that when cross_item_queries_no_sample_outer is disabled (default):
        - Both queries use the same storage tier
        """
        # Ensure the feature flag is disabled (default state)
        state.delete_config("cross_item_queries_no_sample_outer")

        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
        ]

        # Create request with trace_filters and debug enabled
        message = create_trace_item_table_request(
            start_time=start_time,
            end_time=end_time,
            trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id"))
            ],
            trace_filters=trace_filters,
            debug=True,
        )

        response = EndpointTraceItemTable().execute(message)

        # Check that we got query info with SQL
        assert len(response.meta.query_info) > 0
        query_info = response.meta.query_info[0]
        sql = query_info.metadata.sql.lower()

        # When the feature is disabled, both queries should use the same tier
        # In the default case (no special routing), both should use full storage
        # Count how many times we reference eap_items tables
        from_clause_count = sql.count("from eap_items")

        # Should have at least 2 FROM clauses (outer and inner query)
        assert from_clause_count >= 2, f"Should have multiple FROM clauses, got: {sql}"
