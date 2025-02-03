import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, MutableMapping
from unittest.mock import MagicMock, call, patch

import pytest
from clickhouse_driver.errors import ServerException
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    StrArray,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc import RPCEndpoint, run_rpc_handler
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_time_series import (
    EndpointTimeSeries,
    _validate_time_buckets,
)
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events


def gen_span_message(
    dt: datetime, tags: dict[str, str], numerical_attributes: dict[str, float]
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


def store_spans_timeseries(
    start_datetime: datetime,
    period_secs: int,
    len_secs: int,
    metrics: list[DummyMetric],
    tags: dict[str, str] | None = None,
) -> None:
    tags = tags or {}
    messages = []
    for secs in range(0, len_secs, period_secs):
        dt = start_datetime + timedelta(seconds=secs)
        numerical_attributes = {m.name: m.get_value(secs) for m in metrics}
        messages.append(gen_span_message(dt, tags, numerical_attributes))
    spans_storage = get_storage(StorageKey("eap_spans"))
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="p50",
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P95,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="p90",
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

    def test_fails_without_type(self) -> None:
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
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="count",
                ),
            ],
            granularity_secs=60,
        )
        response = self.app.post(
            "/rpc/EndpointTimeSeries/v1", data=message.SerializeToString()
        )
        error = Error()
        if response.status_code != 200:
            error.ParseFromString(response.data)
        assert response.status_code == 400, (error.message, error.details)

    def test_fails_for_logs(self) -> None:
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_LOG,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="count",
                ),
            ],
            granularity_secs=60,
        )
        response = self.app.post(
            "/rpc/EndpointTimeSeries/v1", data=message.SerializeToString()
        )
        error = Error()
        if response.status_code != 200:
            error.ParseFromString(response.data)
        assert response.status_code == 400, (error.message, error.details)

    def test_sum(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 300
        query_duration = 60 * 30
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
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

    def test_with_group_by(self) -> None:
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            tags={"consumer_group": "a", "environment": "prod"},
        )
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 10)],
            tags={"consumer_group": "z", "environment": "prod"},
        )
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 100)],
            tags={"consumer_group": "z", "environment": "dev"},
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="consumer_group"),
                AttributeKey(type=AttributeKey.TYPE_STRING, name="environment"),
            ],
            granularity_secs=300,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, 60 * 30, 300)
        ]

        def sort_key(t: TimeSeries) -> tuple[str, str]:
            return (
                t.group_by_attributes["consumer_group"],
                t.group_by_attributes["environment"],
            )

        assert sorted(response.result_timeseries, key=sort_key) == sorted(
            [
                TimeSeries(
                    label="sum",
                    buckets=expected_buckets,
                    group_by_attributes={"consumer_group": "a", "environment": "prod"},
                    data_points=[
                        DataPoint(data=300, data_present=True)
                        for _ in range(len(expected_buckets))
                    ],
                ),
                TimeSeries(
                    label="sum",
                    buckets=expected_buckets,
                    group_by_attributes={"consumer_group": "z", "environment": "prod"},
                    data_points=[
                        DataPoint(data=3000, data_present=True)
                        for _ in range(len(expected_buckets))
                    ],
                ),
                TimeSeries(
                    label="sum",
                    buckets=expected_buckets,
                    group_by_attributes={"consumer_group": "z", "environment": "dev"},
                    data_points=[
                        DataPoint(data=30000, data_present=True)
                        for _ in range(len(expected_buckets))
                    ],
                ),
            ],
            key=sort_key,
        )

    def test_with_non_string_group_by(self) -> None:
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[
                DummyMetric("test_metric", get_value=lambda x: 1),
                DummyMetric("group_by_metric", get_value=lambda x: 1),
            ],
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_FLOAT, name="group_by_metric"),
            ],
            granularity_secs=300,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, 60 * 30, 300)
        ]

        assert response.result_timeseries == [
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                group_by_attributes={"group_by_metric": "1.0"},
                data_points=[
                    DataPoint(data=300, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            )
        ]

    def test_with_no_data_present(self) -> None:
        granularity_secs = 300
        query_duration = 60 * 30
        store_spans_timeseries(
            BASE_TIME,
            1800,
            3600,
            metrics=[DummyMetric("sparse_metric", get_value=lambda x: 1)],
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sparse_metric"
                    ),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sparse_metric"
                    ),
                    label="avg",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
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
                    DataPoint(data=1, data_present=True),
                    *[DataPoint() for _ in range(len(expected_buckets) - 1)],
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True),
                    *[DataPoint() for _ in range(len(expected_buckets) - 1)],
                ],
            ),
        ]
        pass

    def test_with_filters(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 300
        query_duration = 60 * 30
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            tags={"customer": "bob"},
        )

        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 999)],
            tags={"customer": "alice"},
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
                debug=True,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
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
            filter=TraceItemFilter(
                and_filter=AndFilter(
                    filters=[
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="customer"
                                ),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="bob"),
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="customer"
                                ),
                                op=ComparisonFilter.OP_IN,
                                value=AttributeValue(
                                    val_str_array=StrArray(values=["bob", "alice"])
                                ),
                            )
                        ),
                    ]
                )
            ),
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

    def test_with_unaligned_granularities(self) -> None:
        query_offset = 5
        query_duration = 1800 + query_offset
        granularity_secs = 300
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            tags={"customer": "bob"},
        )
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(
                    seconds=int(BASE_TIME.timestamp()) + query_duration
                ),
                debug=True,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=granularity_secs,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(
                0, query_duration - query_offset + granularity_secs, granularity_secs
            )
        ]
        assert response.result_timeseries == [
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True)
                    for _ in range(len(expected_buckets))
                ],
            )
        ]

    def test_start_time_not_divisible_by_time_buckets_returns_valid_data(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 300
        query_duration = 300
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 1)),
                end_timestamp=Timestamp(
                    seconds=int(BASE_TIME.timestamp() + query_duration + 1)
                ),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)

        ts = response.result_timeseries[0]
        assert len(ts.data_points) == 1
        assert ts.data_points[0].data == 300

    def test_with_non_existent_attribute(self) -> None:
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[
                DummyMetric("test_metric", get_value=lambda x: 1),
            ],
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + 60 * 30)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="non_existent_metric"
                    ),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=300,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, 60 * 30, 300)
        ]

        assert response.result_timeseries == [
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data_present=False) for _ in range(len(expected_buckets))
                ],
            )
        ]

    def test_OOM(self, monkeypatch: Any) -> None:
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="p50",
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P95,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="sentry.duration"
                    ),
                    label="p90",
                ),
            ],
            granularity_secs=60,
        )

        metrics_mock = MagicMock()
        monkeypatch.setattr(RPCEndpoint, "metrics", property(lambda x: metrics_mock))
        with patch(
            "clickhouse_driver.client.Client.execute",
            side_effect=ServerException(
                "DB::Exception: Received from snuba-events-analytics-platform-1-1:1111. DB::Exception: Memory limit (for query) exceeded: would use 1.11GiB (attempt to allocate chunk of 111111 bytes), maximum: 1.11 GiB. Blahblahblahblahblahblahblah",
                code=241,
            ),
        ), patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock:
            resp = run_rpc_handler(
                "EndpointTimeSeries", "v1", message.SerializeToString()
            )
            assert isinstance(resp, ErrorProto)
            assert "DB::Exception: Memory limit (for query) exceeded" in str(
                resp.message
            )

            sentry_sdk_mock.assert_called_once()
            assert metrics_mock.increment.call_args_list.count(call("OOM_query")) == 1


class TestUtils:
    def test_no_duplicate_labels(self) -> None:
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
                debug=True,
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
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=1,
        )

        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTimeSeries().execute(message)

    @pytest.mark.parametrize(
        ("start_ts", "end_ts", "granularity"),
        [
            (BASE_TIME, BASE_TIME + timedelta(hours=1), 1),
            (BASE_TIME, BASE_TIME + timedelta(hours=24), 15),
            (BASE_TIME, BASE_TIME + timedelta(hours=1), 0),
            (BASE_TIME + timedelta(hours=1), BASE_TIME, 0),
            (BASE_TIME, BASE_TIME + timedelta(hours=1), 3 * 3600),
        ],
    )
    def test_bad_granularity(
        self, start_ts: datetime, end_ts: datetime, granularity: int
    ) -> None:
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(start_ts.timestamp())),
                end_timestamp=Timestamp(seconds=int(end_ts.timestamp())),
                debug=True,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=granularity,
        )

        with pytest.raises(BadSnubaRPCRequestException):
            _validate_time_buckets(message)

    def test_adjust_buckets(self) -> None:
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp()) + 65),
                debug=True,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=15,
        )

        _validate_time_buckets(message)
        # add another bucket to fit into granularity_secs
        assert message.meta.end_timestamp.seconds == int(BASE_TIME.timestamp()) + 75
