from datetime import datetime, timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_trace_item_stats_pb2 import (
    AttributeDistribution,
    AttributeDistributionsRequest,
    StatsType,
    TraceItemStatsRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_trace_item_stats import EndpointTraceItemStats
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_endpoint_trace_item_table.test_endpoint_trace_item_table_logs import (
    gen_log_message,
)

BASE_TIME = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(
    minutes=180
)


@pytest.fixture(autouse=False)
def setup_logs_in_db(clickhouse_db: None, redis_db: None) -> None:
    logs_storage = get_storage(StorageKey("eap_items_log"))
    messages = []
    for i in range(120):
        messages.append(
            gen_log_message(
                dt=BASE_TIME - timedelta(minutes=i),
                body=f"hello world {i}",
                tags={
                    "bool_tag": i % 2 == 0,
                    "int_tag": i,
                    "double_tag": float(i) / 2.0,
                    "str_tag": f"num: {i}",
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
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        message = TraceItemStatsRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=hour_ago),
                end_timestamp=ts,
                request_id="be3123b3-2e5d-4eb9-bb48-f38eaa9e8480",
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
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
        assert len(actual) == 3
        assert actual[0:2] == [
            AttributeDistribution(
                attribute_name="sentry.severity_text",
                buckets=[AttributeDistribution.Bucket(label="INFO", value=60)],
            ),
            AttributeDistribution(
                attribute_name="sentry.span_id",
                buckets=[
                    AttributeDistribution.Bucket(label="123456781234567D", value=60)
                ],
            ),
        ]
        assert actual[2].attribute_name == "sentry.body"
        assert sorted(
            actual[2].buckets, key=lambda x: int(x.label[len("hello world ") :])
        ) == [
            AttributeDistribution.Bucket(label=f"hello world {i}", value=1)
            for i in range(1, 61)
        ]
