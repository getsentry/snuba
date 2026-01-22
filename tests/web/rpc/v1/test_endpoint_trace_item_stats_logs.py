from datetime import timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistribution,
    AttributeDistributionsRequest,
    StatsType,
    TraceItemStatsRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_stats import EndpointTraceItemStats
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import BASE_TIME, START_TIMESTAMP, gen_item_message


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items"))
    messages = []
    for i in range(120):
        timestamp = BASE_TIME + timedelta(minutes=i)
        messages.append(
            gen_item_message(
                start_timestamp=timestamp,
                remove_default_attributes=True,
                type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                attributes={
                    "bool_tag": AnyValue(bool_value=i % 2 == 0),
                    "double_tag": AnyValue(double_value=float(i) / 2.0),
                    "int_tag": AnyValue(int_value=i),
                    "sentry.body": AnyValue(string_value=f"hello world {i}"),
                    "sentry.severity_number": AnyValue(int_value=10),
                    "sentry.severity_text": AnyValue(string_value="info"),
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTraceItemStatsForLogs(BaseApiTest):
    def test_with_logs_data(self, setup_logs_in_db: Any) -> None:
        """
        ensure a traceitemstats request for logs successfully executes without error
        """
        end_timestamp = Timestamp()
        end_timestamp.FromDatetime(BASE_TIME + timedelta(hours=1))
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=START_TIMESTAMP,
                end_timestamp=end_timestamp,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_HIGHEST_ACCURACY,
                ),
            ),
            stats_types=[
                StatsType(
                    attribute_distributions=AttributeDistributionsRequest(
                        max_buckets=100, max_attributes=1000
                    )
                )
            ],
        )
        response = EndpointTraceItemStats().execute(message)
        assert response.results[0].HasField("attribute_distributions")
        actual = response.results[0].attribute_distributions.attributes
        assert len(actual) == 2
        assert actual[0] == AttributeDistribution(
            attribute_name="sentry.severity_text",
            buckets=[AttributeDistribution.Bucket(label="info", value=60)],
        )
        assert actual[1].attribute_name == "sentry.body"
        assert sorted(actual[1].buckets, key=lambda x: int(x.label[len("hello world ") :])) == [
            AttributeDistribution.Bucket(label=f"hello world {i}", value=1) for i in range(60)
        ]
