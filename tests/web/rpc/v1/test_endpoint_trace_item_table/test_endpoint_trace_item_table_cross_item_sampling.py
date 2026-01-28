from datetime import datetime
from functools import wraps
from typing import Any, ContextManager, List
from unittest.mock import Mock, patch

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
from snuba.datasets.storages.storage_key import StorageKey
from snuba.downsampled_storage_tiers import Tier
from snuba.web.rpc import RPCEndpoint
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


def track_storage_selections() -> tuple[List[StorageKey], ContextManager[None]]:
    """
    Context manager that tracks storage selections during query execution.
    Returns a tuple of (storage_keys list, context manager).
    """
    storage_keys: List[StorageKey] = []

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

    return storage_keys, patch.object(
        EAPItemsStorageSelector,
        "select_storage",
        track_storage_selection,  # type: ignore[arg-type]
    )


@pytest.mark.eap
@pytest.mark.redis_db
class TestTraceItemTableCrossItemSampling(BaseApiTest):
    def test_cross_item_query_sampling_enabled(self) -> None:
        """
        Test that when cross_item_queries_no_sample_outer is enabled:
        - The inner query uses downsampled storage (TIER_8)
        - The outer query uses full storage (EAP_ITEMS)
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

        # Create a mock routing decision with TIER_8
        mock_routing_decision = Mock()
        mock_routing_decision.tier = Tier.TIER_8
        mock_routing_decision.can_run = True
        mock_routing_decision.time_window = None
        mock_routing_decision.strategy = Mock()
        mock_routing_decision.strategy.merge_clickhouse_settings = Mock(return_value={})
        mock_routing_decision.strategy.after_execute = Mock()

        storage_keys, storage_tracker = track_storage_selections()

        with storage_tracker:
            with patch.object(RPCEndpoint, "_RPCEndpoint__before_execute"):
                message = create_trace_item_table_request(
                    start_time=start_time,
                    end_time=end_time,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    columns=[
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id")
                        )
                    ],
                    trace_filters=trace_filters,
                )

                endpoint = EndpointTraceItemTable()
                endpoint.routing_decision = mock_routing_decision
                endpoint.execute(message)

                # Verify storages were selected (should have at least 2 calls: inner + outer)
                assert len(storage_keys) >= 2, (
                    f"Expected at least 2 storage selections, got {len(storage_keys)}"
                )

                # The inner query should use downsampled storage (TIER_8)
                assert StorageKey.EAP_ITEMS_DOWNSAMPLE_8 in storage_keys, (
                    f"Inner query should use EAP_ITEMS_DOWNSAMPLE_8, got: {storage_keys}"
                )

                # The outer query should use full storage (EAP_ITEMS)
                assert StorageKey.EAP_ITEMS in storage_keys, (
                    f"Outer query should use EAP_ITEMS, got: {storage_keys}"
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

        # Create a mock routing decision with TIER_8
        mock_routing_decision = Mock()
        mock_routing_decision.tier = Tier.TIER_8
        mock_routing_decision.can_run = True
        mock_routing_decision.time_window = None
        mock_routing_decision.strategy = Mock()
        mock_routing_decision.strategy.merge_clickhouse_settings = Mock(return_value={})
        mock_routing_decision.strategy.after_execute = Mock()

        storage_keys, storage_tracker = track_storage_selections()

        with storage_tracker:
            with patch.object(RPCEndpoint, "_RPCEndpoint__before_execute"):
                message = create_trace_item_table_request(
                    start_time=start_time,
                    end_time=end_time,
                    trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                    columns=[
                        Column(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.span_id")
                        )
                    ],
                    trace_filters=trace_filters,
                )

                endpoint = EndpointTraceItemTable()
                endpoint.routing_decision = mock_routing_decision
                endpoint.execute(message)

                # When feature is disabled, both inner and outer queries should use the same tier
                assert len(storage_keys) >= 2, (
                    f"Expected at least 2 storage selections, got {len(storage_keys)}"
                )

                # All storages should be TIER_8 (downsampled)
                assert all(key == StorageKey.EAP_ITEMS_DOWNSAMPLE_8 for key in storage_keys), (
                    f"All queries should use EAP_ITEMS_DOWNSAMPLE_8, got: {storage_keys}"
                )
