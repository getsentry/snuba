import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    Function,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events

_TRACE_ID = uuid.uuid4().hex


def gen_message(
    dt: datetime,
) -> Mapping[str, Any]:
    return {
        "organization_id": 1,
        "project_id": 1,
        "retention_days": 30,
        "region": "global",
        "environment": "prod",
        "subscription_id": uuid.uuid4().hex,
        "guid": uuid.uuid4().hex,
        "scheduled_check_time_ms": int(dt.timestamp() * 1e3),
        "actual_check_time_ms": int(dt.timestamp() * 1e3),
        "duration_ms": 1000,
        "status": "missed_window",
        "status_reason": {
            "type": "success",
            "description": "Some",
        },
        "http_status_code": 200,
        "trace_id": _TRACE_ID,
        "request_info": {
            "request_type": "GET",
            "http_status_code": 200,
        },
    }


BASE_TIME = datetime.utcnow().replace(
    hour=8,
    minute=0,
    second=0,
    microsecond=0,
    tzinfo=UTC,
) - timedelta(hours=24)


def store_timeseries(
    start_datetime: datetime,
    period_secs: int,
    len_secs: int,
) -> None:
    messages = []
    for secs in range(0, len_secs, period_secs):
        dt = start_datetime + timedelta(seconds=secs)
        messages.append(gen_message(dt))
    uptime_checks_storage = get_storage(StorageKey("uptime_monitor_checks"))
    write_raw_unprocessed_events(uptime_checks_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApi(BaseApiTest):
    def test_basic(self) -> None:
        ts = Timestamp()
        ts.GetCurrentTime()
        tstart = Timestamp(seconds=ts.seconds - 3600)
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=tstart,
                end_timestamp=ts,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="uptime_check_id"
                    ),
                    label="count",
                ),
            ],
            granularity_secs=60,
        )
        response = self.app.post(
            "/rpc/EndpointTimeSeries/v1", data=message.SerializeToString()
        )
        if response.status_code != 200:
            error = Error()
            error.ParseFromString(response.data)
            assert response.status_code == 200, (error.message, error.details)

    def test_count(self) -> None:
        store_timeseries(
            BASE_TIME,
            1,
            3600,
        )

        granularity_secs = 300
        query_duration = 60 * 30
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(
                    seconds=int(BASE_TIME.timestamp() + query_duration)
                ),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="uptime_check_id"
                    ),
                    label="count",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]

        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="count",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_with_group_by(self) -> None:
        store_timeseries(
            BASE_TIME,
            1,
            3600,
        )

        granularity_secs = 300
        query_duration = 60 * 30
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(
                    seconds=int(BASE_TIME.timestamp() + query_duration)
                ),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_UPTIME_CHECK,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_STRING, name="uptime_check_id"
                    ),
                    label="count",
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="region"),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]

        assert response.result_timeseries == [
            TimeSeries(
                label="count",
                buckets=expected_buckets,
                group_by_attributes={"region": "global"},
                data_points=[
                    DataPoint(data=300, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_random(self) -> None:
        store_timeseries(
            datetime.fromisoformat("2025-01-17T23:03:02.220914Z"),
            1,
            3600,
        )
        from google.protobuf.json_format import Parse

        json_str = """{
  "meta": {
    "organizationId": "1",
    "projectIds": [
      "1"
    ],
    "startTimestamp": "2025-01-13T23:03:02.220914Z",
    "endTimestamp": "2025-01-27T23:03:02.220929Z",
    "traceItemName": 4,
    "traceItemType": "TRACE_ITEM_TYPE_UPTIME_CHECK"
  },
  "aggregations": [
    {
      "aggregate": "FUNCTION_COUNT",
      "key": {
        "type": "TYPE_FLOAT",
        "name": "uptime_check_id"
      }
    }
  ],
  "granularitySecs": "3600",
  "groupBy": [
    {
      "type": "TYPE_STRING",
      "name": "uptime_subscription_id"
    }
  ]
}"""
        message = Parse(json_str, TimeSeriesRequest())
        res = self.app.post(
            "/rpc/EndpointTimeSeries/v1", data=message.SerializeToString()
        )
        print(res)
