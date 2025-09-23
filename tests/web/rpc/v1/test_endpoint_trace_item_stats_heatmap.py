from datetime import timedelta
from typing import Any

import pytest
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    HeatmapRequest,
    MatrixColumn,
    StatsType,
    TraceItemStatsRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_stats import EndpointTraceItemStats
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import (
    BASE_TIME,
    END_TIMESTAMP,
    START_TIMESTAMP,
    gen_item_message,
)


@pytest.fixture(autouse=False)
def setup_heatmap_teardown(clickhouse_db: None, redis_db: None) -> None:
    """
    Setup test data for heatmap tests.
    Creates 100 trace items with varied attributes for testing heatmap functionality:
    - span.op: "db.query" (40 items), "http.server" (30 items), "cache.get" (30 items)
    - duration: distributed across 0-400ms range
    - status_code: 200, 404, 500
    - numeric_attr: values 0-99 for numeric bucketing tests
    """
    items_storage = get_storage(StorageKey("eap_items"))
    messages = []

    for i in range(100):
        # Create varied span operations
        if i < 40:
            span_op = "db.query"
            duration_ms = (i % 4) * 100 + 50  # 50, 150, 250, 350
        elif i < 70:
            span_op = "http.server"
            duration_ms = (i % 3) * 100 + 25  # 25, 125, 225
        else:
            span_op = "cache.get"
            duration_ms = (i % 2) * 150 + 75  # 75, 225

        # Create varied status codes
        if i % 3 == 0:
            status_code = "200"
        elif i % 3 == 1:
            status_code = "404"
        else:
            status_code = "500"

        messages.append(
            gen_item_message(
                start_timestamp=BASE_TIME - timedelta(minutes=i),
                attributes={
                    "span.op": AnyValue(string_value=span_op),
                    "eap.duration": AnyValue(int_value=duration_ms),
                    "http.status_code": AnyValue(string_value=status_code),
                    "numeric_attr": AnyValue(int_value=i),
                    "category": AnyValue(string_value="performance"),
                },
                remove_default_attributes=True,
            )
        )

    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemStatsHeatmap(BaseApiTest):
    def test_basic_heatmap_no_data(self) -> None:
        """Test basic heatmap request without any data in the database."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                        y_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration"),
                        num_y_buckets=4,
                    )
                )
            ],
        )

        response = self.app.post("/rpc/EndpointTraceItemStats/v1", data=message.SerializeToString())
        error_proto = ErrorProto()
        if response.status_code != 200:
            error_proto.ParseFromString(response.data)
        assert response.status_code == 200, error_proto

    def test_heatmap_with_string_x_numeric_y(self, setup_heatmap_teardown: Any) -> None:
        """Test heatmap with string x-axis (span.op) and numeric y-axis (duration) bucketing."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                        y_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration"),
                        num_y_buckets=4,
                    )
                )
            ],
        )

        response = EndpointTraceItemStats().execute(message)
        assert len(response.results) == 1
        heatmap = response.results[0].heatmap

        # Verify heatmap structure
        assert heatmap.x_attribute == AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op")
        assert heatmap.y_attribute == AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration")
        assert heatmap.y_buckets == [
            AttributeValue(val_str="[25, 106.25)"),
            AttributeValue(val_str="[106.25, 187.5)"),
            AttributeValue(val_str="[187.5, 268.75)"),
            AttributeValue(val_str="[268.75, 350]"),
        ]
        assert heatmap.data == [
            MatrixColumn(
                x_label=AttributeValue(val_str="db.query"),
                values=[10, 10, 10, 10],
            ),
            MatrixColumn(
                x_label=AttributeValue(val_str="http.server"),
                values=[10, 10, 10, 0],
            ),
            MatrixColumn(
                x_label=AttributeValue(val_str="cache.get"),
                values=[15, 0, 15, 0],
            ),
        ]

    def test_heatmap_with_filter(self, setup_heatmap_teardown: Any) -> None:
        """Test heatmap with filters applied."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="http.status_code"),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="200"),
                )
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                        y_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration"),
                        num_y_buckets=4,
                    )
                )
            ],
        )

        response = EndpointTraceItemStats().execute(message)
        assert len(response.results) == 1
        assert response.results[0].HasField("heatmap")
        heatmap = response.results[0].heatmap

        # With status_code=200 filter, we should have roughly 1/3 of the data
        # (every 3rd item has status_code=200)
        total_count = 0.0
        for column in heatmap.data:
            total_count += sum(column.values)

        # Should be approximately 33-34 items (100/3)
        assert 30 <= total_count <= 37

    def test_heatmap_numeric_x_and_y_buckets(self, setup_heatmap_teardown: Any) -> None:
        """Test heatmap with numeric bucketing on both x and y axes."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="numeric_attr"),
                        y_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration"),
                        num_x_buckets=5,  # Bucket numeric_attr (0-99) into 5 buckets
                        num_y_buckets=4,  # Bucket duration into 4 buckets
                    )
                )
            ],
        )

        response = EndpointTraceItemStats().execute(message)
        assert len(response.results) == 1
        heatmap = response.results[0].heatmap

        # Verify heatmap structure for numeric bucketing
        assert heatmap.x_attribute == AttributeKey(type=AttributeKey.TYPE_INT, name="numeric_attr")
        assert heatmap.y_attribute == AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration")
        assert heatmap.y_buckets == [
            AttributeValue(val_str="[25, 106.25)"),
            AttributeValue(val_str="[106.25, 187.5)"),
            AttributeValue(val_str="[187.5, 268.75)"),
            AttributeValue(val_str="[268.75, 350]"),
        ]
        assert heatmap.data == [
            MatrixColumn(x_label=AttributeValue(val_str="[0,19.8)"), values=[5, 5, 5, 4]),
            MatrixColumn(x_label=AttributeValue(val_str="[19.8,39.6)"), values=[6, 5, 5, 4]),
            MatrixColumn(x_label=AttributeValue(val_str="[39.6,59.4)"), values=[11, 10, 10, 0]),
            MatrixColumn(x_label=AttributeValue(val_str="[59.4,79.2)"), values=[10, 10, 10, 0]),
            MatrixColumn(x_label=AttributeValue(val_str="[79.2,99]"), values=[10, 10, 10, 10]),
        ]

    def test_heatmap_string_x_string_y(self, setup_heatmap_teardown: Any) -> None:
        """Test heatmap with string attributes on both axes."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                        y_attribute=AttributeKey(
                            type=AttributeKey.TYPE_STRING, name="http.status_code"
                        ),
                        # No bucketing needed for string attributes
                    )
                )
            ],
        )

        response = EndpointTraceItemStats().execute(message)
        assert len(response.results) == 1
        heatmap = response.results[0].heatmap

        # Verify heatmap structure
        assert heatmap.x_attribute == AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op")
        assert heatmap.y_attribute == AttributeKey(
            type=AttributeKey.TYPE_STRING, name="http.status_code"
        )
        assert heatmap.y_buckets == [
            AttributeValue(val_str="200"),
            AttributeValue(val_str="404"),
            AttributeValue(val_str="500"),
        ]
        assert heatmap.data == [
            MatrixColumn(x_label=AttributeValue(val_str="db.query"), values=[14, 13, 13]),
            MatrixColumn(x_label=AttributeValue(val_str="db.cache"), values=[10, 10, 10]),
            MatrixColumn(x_label=AttributeValue(val_str="cache.get"), values=[10, 10, 10]),
        ]

    def test_heatmap_empty_buckets(self, setup_heatmap_teardown: Any) -> None:
        """Test heatmap handles empty buckets correctly."""
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=END_TIMESTAMP,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            # Filter to get only a subset of data that will create empty buckets
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="db.query"),
                )
            ),
            stats_types=[
                StatsType(
                    heatmap=HeatmapRequest(
                        x_attribute=AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op"),
                        y_attribute=AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration"),
                        num_y_buckets=10,  # More buckets than data points
                    )
                )
            ],
        )

        response = EndpointTraceItemStats().execute(message)
        assert len(response.results) == 1
        heatmap = response.results[0].heatmap

        # Should only have db.query column
        assert heatmap.x_attribute == AttributeKey(type=AttributeKey.TYPE_STRING, name="span.op")
        assert heatmap.y_attribute == AttributeKey(type=AttributeKey.TYPE_INT, name="eap.duration")
        assert heatmap.y_buckets == [
            AttributeValue(val_str="[50, 80)"),
            AttributeValue(val_str="[80, 110)"),
            AttributeValue(val_str="[110, 140)"),
            AttributeValue(val_str="[140, 170)"),
            AttributeValue(val_str="[170, 200)"),
            AttributeValue(val_str="[200, 230)"),
            AttributeValue(val_str="[230, 260)"),
            AttributeValue(val_str="[260, 290)"),
            AttributeValue(val_str="[290, 320)"),
            AttributeValue(val_str="[320, 350]"),
        ]
        assert heatmap.data == [
            MatrixColumn(
                x_label=AttributeValue(val_str="db.query"),
                values=[10, 0, 0, 10, 0, 0, 10, 0, 0, 10],
            ),
        ]
