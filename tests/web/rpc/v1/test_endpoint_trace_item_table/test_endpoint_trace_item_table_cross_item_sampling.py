from datetime import datetime
from functools import wraps
from typing import Any
from unittest.mock import patch

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
class TestTraceItemTableCrossItemSampling(BaseApiTest):
    def test_cross_item_query_sampling_enabled(self) -> None:
        """
        Test that when cross_item_queries_no_sample_outer is enabled:
        - The query executes successfully with trace_filters
        - Both inner and outer queries can use different storage tiers
        """
        # Enable the feature flag
        state.set_config("cross_item_queries_no_sample_outer", 1)

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

        # Track what storages are selected
        storage_keys = []

        from snuba.datasets.entities.storage_selectors.eap_items import (
            EAPItemsStorageSelector,
        )

        original_select_storage = EAPItemsStorageSelector.select_storage

        @wraps(original_select_storage)
        def track_storage_selection(
            self: Any, query: Any, query_settings: Any, storage_connections: Any
        ) -> Any:
            selected = original_select_storage(self, query, query_settings, storage_connections)
            storage_keys.append(selected.storage.get_storage_key())
            return selected

        # Patch the storage selector to track what storages are used
        with patch.object(EAPItemsStorageSelector, "select_storage", track_storage_selection):
            # Create request with trace_filters
            message = create_trace_item_table_request(
                start_time=start_time,
                end_time=end_time,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                columns=[
                    Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id"))
                ],
                trace_filters=trace_filters,
            )

            response = EndpointTraceItemTable().execute(message)

            # Verify the query executed successfully
            assert response is not None

            # Verify storages were selected (should have at least 2 calls: inner + outer)
            assert len(storage_keys) >= 2, (
                f"Expected at least 2 storage selections, got {len(storage_keys)}"
            )

    def test_cross_item_query_sampling_disabled(self) -> None:
        """
        Test that when cross_item_queries_no_sample_outer is disabled (default):
        - Both queries use the same storage tier
        """
        trace_ids, all_items, start_time, end_time = create_cross_item_test_data()
        write_cross_item_data_to_storage(all_items)

        trace_filters = [
            trace_filter(
                comparison_filter("span.attr1", "val1"),
                TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
        ]

        # Track what storages are selected
        storage_keys = []

        from snuba.datasets.entities.storage_selectors.eap_items import (
            EAPItemsStorageSelector,
        )

        original_select_storage = EAPItemsStorageSelector.select_storage

        @wraps(original_select_storage)
        def track_storage_selection(
            self: Any, query: Any, query_settings: Any, storage_connections: Any
        ) -> Any:
            selected = original_select_storage(self, query, query_settings, storage_connections)
            storage_keys.append(selected.storage.get_storage_key())
            return selected

        # Patch the storage selector to track what storages are used
        with patch.object(EAPItemsStorageSelector, "select_storage", track_storage_selection):
            # Create request with trace_filters
            message = create_trace_item_table_request(
                start_time=start_time,
                end_time=end_time,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                columns=[
                    Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id"))
                ],
                trace_filters=trace_filters,
            )

            EndpointTraceItemTable().execute(message)

            # When feature is disabled, both inner and outer queries should use the same tier
            assert len(storage_keys) >= 2, (
                f"Expected at least 2 storage selections, got {len(storage_keys)}"
            )

            # All storages should be the same
            assert all(key == storage_keys[0] for key in storage_keys), (
                f"All queries should use the same storage tier, got: {storage_keys}"
            )
