import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, MutableMapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta
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


def gen_message(
    dt: datetime,
    tags: dict[str, str],
    numerical_attributes: dict[str, float],
    measurements: dict[str, float],
) -> MutableMapping[str, Any]:
    return {
        "description": "/api/0/relays/projectconfigs/",
        "duration_ms": 152,
        "event_id": "d826225de75d42d6b2f01b957d51f18f",
        "exclusive_time_ms": 0.228,
        "is_segment": True,
        "data": {
            "sentry.environment": "development",
            "sentry.release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
            "thread.name": "uWSGIWorker1Core0",
            "thread.id": "8522009600",
            "sentry.segment.name": "/api/0/relays/projectconfigs/",
            "sentry.sdk.name": "sentry.python.django",
            "sentry.sdk.version": "2.7.0",
            **numerical_attributes,
        },
        "measurements": {
            "num_of_spans": {"value": 50.0},
            "client_sample_rate": {"value": 1},
            **measurements,
        },
        "organization_id": 1,
        "origin": "auto.http.django",
        "project_id": 1,
        "received": 1721319572.877828,
        "retention_days": 90,
        "segment_id": "8873a98879faf06d",
        "sentry_tags": {
            "category": "http",
            "environment": "development",
            "op": "http.server",
            "platform": "python",
            "release": "backend@24.7.0.dev0+c45b49caed1e5fcbf70097ab3f434b487c359b6b",
            "sdk.name": "sentry.python.django",
            "sdk.version": "2.7.0",
            "status": "ok",
            "status_code": "200",
            "thread.id": "8522009600",
            "thread.name": "uWSGIWorker1Core0",
            "trace.status": "ok",
            "transaction": "/api/0/relays/projectconfigs/",
            "transaction.method": "POST",
            "transaction.op": "http.server",
            "user": "ip:127.0.0.1",
        },
        "span_id": uuid.uuid4().hex,
        "tags": tags,
        "trace_id": uuid.uuid4().hex,
        "start_timestamp_ms": int(dt.timestamp()) * 1000,
        "start_timestamp_precise": dt.timestamp(),
        "end_timestamp_precise": dt.timestamp() + 1,
    }


BASE_TIME = datetime.utcnow().replace(
    hour=8, minute=0, second=0, microsecond=0, tzinfo=UTC
) - timedelta(hours=24)


SecsFromSeriesStart = int


@dataclass
class DummyMetric:
    name: str
    get_value: Callable[[SecsFromSeriesStart], float]


@dataclass
class DummyMeasurement:
    name: str
    get_value: Callable[[SecsFromSeriesStart], float]


def store_timeseries(
    start_datetime: datetime,
    period_secs: int,
    len_secs: int,
    metrics: list[DummyMetric],
    tags: dict[str, str] | None = None,
    measurements: list[DummyMeasurement] = [],
) -> None:
    tags = tags or {}
    messages = []
    for secs in range(0, len_secs, period_secs):
        dt = start_datetime + timedelta(seconds=secs)
        numerical_attributes = {m.name: m.get_value(secs) for m in metrics}
        measurements = {m.name: m.get_value(secs) for m in measurements}
        messages.append(gen_message(dt, tags, numerical_attributes, measurements))
    spans_storage = get_storage(StorageKey("eap_spans"))
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApiWithExtrapolation(BaseApiTest):
    def test_count(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 300
        query_duration = 60 * 30
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            measurements=[
                DummyMeasurement("client_sample_rate", get_value=lambda s: 1)
            ],
        )

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
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="count",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="count",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
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
                label="avg",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]
