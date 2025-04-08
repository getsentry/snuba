from datetime import datetime, timedelta
from typing import Any

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
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
    for i in range(240):
        # 1 log every 30s for the 2 hours
        messages.append(
            gen_log_message(
                dt=BASE_TIME - timedelta(seconds=30 * i),
                body=f"hello world {i}",
                tags={
                    "bool_tag": i % 2 == 0,
                    "int_tag": i,
                    "double_tag": float(i) / 2.0,
                    "str_tag": f"num: {i}",
                    "test_metric": 1.0,
                },
            )
        )
    write_raw_unprocessed_events(logs_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesForLogs(BaseApiTest):
    def test_with_logs_data(self, setup_logs_in_db: Any) -> None:
        ts = Timestamp(seconds=int(BASE_TIME.timestamp()))
        hour_ago = int((BASE_TIME - timedelta(hours=1)).timestamp())
        granularity_secs = 60
        query_duration_secs = 60 * 60  # 60 minutes

        message = TimeSeriesRequest(
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
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="avg",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int((BASE_TIME - timedelta(hours=1)).timestamp()) + secs)
            for secs in range(0, query_duration_secs, granularity_secs)
        ]
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="avg",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True, sample_count=2)
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=2, data_present=True, sample_count=2)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]
