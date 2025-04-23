import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, MutableMapping
from unittest.mock import MagicMock, call, patch

import pytest
from clickhouse_driver.errors import ServerException
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.downsampled_storage_pb2 import (
    DownsampledStorageConfig,
    DownsampledStorageMeta,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    Expression,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error
from sentry_protos.snuba.v1.formula_pb2 import Literal
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
from snuba.web import QueryException
from snuba.web.rpc import RPCEndpoint
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
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


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

    def test_conditional_aggregation(self) -> None:
        # store a test metric with a value of 1, for ever even second of one hour
        granularity_secs = 300
        query_duration = 60 * 30
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: int(x % 2 == 0))],
        )

        test_metric_attribute_key = AttributeKey(
            type=AttributeKey.TYPE_FLOAT, name="test_metric"
        )
        test_metric_is_one_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=test_metric_attribute_key,
                op=ComparisonFilter.OP_EQUALS,
                value=AttributeValue(val_int=1),
            )
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
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=test_metric_attribute_key,
                                label="sum",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                filter=test_metric_is_one_filter,
                            )
                        ),
                        right=Expression(
                            conditional_aggregation=AttributeConditionalAggregation(
                                aggregate=Function.FUNCTION_AVG,
                                key=test_metric_attribute_key,
                                label="avg",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                filter=test_metric_is_one_filter,
                            )
                        ),
                    ),
                    label="sum + avg",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]

        expected_avg_timeseries = TimeSeries(
            label="avg",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=1, data_present=True, sample_count=150)
                for _ in range(len(expected_buckets))
            ],
        )
        expected_sum_timeseries = TimeSeries(
            label="sum",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=150, data_present=True)
                for _ in range(len(expected_buckets))
            ],
        )
        expected_formula_timeseries = TimeSeries(
            label="sum + avg",
            buckets=expected_buckets,
            data_points=[
                DataPoint(
                    data=sum_datapoint.data + avg_datapoint.data,
                    data_present=True,
                    sample_count=sum_datapoint.sample_count,
                )
                for sum_datapoint, avg_datapoint in zip(
                    expected_sum_timeseries.data_points,
                    expected_avg_timeseries.data_points,
                )
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            expected_formula_timeseries
        ]

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
                    DataPoint(data=1, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True, sample_count=300)
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
                        DataPoint(data=300, data_present=True, sample_count=300)
                        for _ in range(len(expected_buckets))
                    ],
                ),
                TimeSeries(
                    label="sum",
                    buckets=expected_buckets,
                    group_by_attributes={"consumer_group": "z", "environment": "prod"},
                    data_points=[
                        DataPoint(data=3000, data_present=True, sample_count=300)
                        for _ in range(len(expected_buckets))
                    ],
                ),
                TimeSeries(
                    label="sum",
                    buckets=expected_buckets,
                    group_by_attributes={"consumer_group": "z", "environment": "dev"},
                    data_points=[
                        DataPoint(data=30000, data_present=True, sample_count=300)
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
                    DataPoint(data=300, data_present=True, sample_count=300)
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
                    DataPoint(data=1, data_present=True, sample_count=1),
                    *[DataPoint() for _ in range(len(expected_buckets) - 1)],
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True, sample_count=1),
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
                    DataPoint(data=1, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_with_filters_ignore_case(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 300
        query_duration = 60 * 30
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            tags={"customer": "bOb"},
        )

        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 999)],
            tags={"customer": "aLiCe"},
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
                                value=AttributeValue(val_str="BoB"),
                                ignore_case=True,
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_STRING, name="customer"
                                ),
                                op=ComparisonFilter.OP_IN,
                                value=AttributeValue(
                                    val_str_array=StrArray(values=["BOB", "AlIcE"])
                                ),
                                ignore_case=True,
                            )
                        ),
                    ]
                )
            ),
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        # print(response)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="avg",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True, sample_count=300)
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
                    DataPoint(data=300, data_present=True, sample_count=300)
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
            with pytest.raises(QueryException) as e:
                EndpointTimeSeries().execute(message)
            assert "DB::Exception: Memory limit (for query) exceeded" in str(e.value)

            sentry_sdk_mock.assert_called()
            assert metrics_mock.increment.call_args_list.count(call("OOM_query")) == 1

    def test_formula(self) -> None:
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
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="test_metric"
                                ),
                                label="sum",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_AVG,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="test_metric"
                                ),
                                label="avg",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            )
                        ),
                    ),
                    label="sum + avg",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_avg_timeseries = TimeSeries(
            label="avg",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=1, data_present=True, sample_count=300)
                for _ in range(len(expected_buckets))
            ],
        )
        expected_sum_timeseries = TimeSeries(
            label="sum",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=300, data_present=True)
                for _ in range(len(expected_buckets))
            ],
        )
        expected_formula_timeseries = TimeSeries(
            label="sum + avg",
            buckets=expected_buckets,
            data_points=[
                DataPoint(
                    data=sum_datapoint.data + avg_datapoint.data,
                    data_present=True,
                    sample_count=sum_datapoint.sample_count,
                )
                for sum_datapoint, avg_datapoint in zip(
                    expected_sum_timeseries.data_points,
                    expected_avg_timeseries.data_points,
                )
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            expected_formula_timeseries
        ]

    def test_eap_items_name_attribute(self) -> None:
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
            expressions=[
                Expression(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_FLOAT, name="test_metric"
                        ),
                        label="sum(test_metric)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label="sum(test_metric)",
                ),
            ],
            group_by=[
                AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.name"),
            ],
            granularity_secs=granularity_secs,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_timeseries = TimeSeries(
            label="sum(test_metric)",
            group_by_attributes={"sentry.name": "/api/0/relays/projectconfigs/"},
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=300, data_present=True, sample_count=300)
                for _ in range(len(expected_buckets))
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            expected_timeseries
        ]

    def test_formula_default_value(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric_a", get_value=lambda x: 2)],
        )
        store_spans_timeseries(
            BASE_TIME,
            1,
            1800,
            metrics=[DummyMetric("test_metric_b", get_value=lambda x: 1)],
        )
        granularity_secs = 60 * 10
        query_duration_secs = 60 * 60
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(
                    seconds=int(BASE_TIME.timestamp() + query_duration_secs)
                ),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_DIVIDE,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="test_metric_a"
                                ),
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="test_metric_b"
                                ),
                            )
                        ),
                        default_value_double=-1.0,
                    ),
                    label="a / b",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration_secs, granularity_secs)
        ]
        expected_timeseries = TimeSeries(
            label="a / b",
            buckets=expected_buckets,
            data_points=[
                DataPoint(
                    data=2,
                    data_present=True,
                ),
                DataPoint(
                    data=2,
                    data_present=True,
                ),
                DataPoint(
                    data=2,
                    data_present=True,
                ),
                DataPoint(data=-1.0, data_present=True),
                DataPoint(data=-1.0, data_present=True),
                DataPoint(data=-1.0, data_present=True),
            ],
        )
        assert response.result_timeseries == [expected_timeseries]

    def test_literal(self) -> None:
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
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="test_metric"
                                ),
                                label="sum",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            )
                        ),
                        right=Expression(
                            literal=Literal(val_double=1.0),
                        ),
                    ),
                    label="sum + 1",
                ),
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_DIVIDE,
                        left=Expression(
                            literal=Literal(val_double=1.0),
                        ),
                        right=Expression(
                            literal=Literal(val_double=2.0),
                        ),
                    ),
                    label="1 / 2",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_sum_timeseries = TimeSeries(
            label="sum",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=300, data_present=True)
                for _ in range(len(expected_buckets))
            ],
        )
        expected_formula_timeseries = TimeSeries(
            label="sum + 1",
            buckets=expected_buckets,
            data_points=[
                DataPoint(
                    data=sum_datapoint.data + 1,
                    data_present=True,
                    sample_count=sum_datapoint.sample_count,
                )
                for sum_datapoint in expected_sum_timeseries.data_points
            ],
        )

        expected_literal_timeseries = TimeSeries(
            label="1 / 2",
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=0.5, data_present=True)
                for _ in range(len(expected_buckets))
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            expected_literal_timeseries,
            expected_formula_timeseries,
        ]

    def test_preflight(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 3600
        query_duration = granularity_secs * 1
        store_spans_timeseries(
            BASE_TIME,
            1,
            query_duration,
            metrics=[DummyMetric("test_preflight_metric", get_value=lambda x: 1)],
        )

        aggregations = [
            AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(
                    type=AttributeKey.TYPE_FLOAT, name="test_preflight_metric"
                ),
                label="sum",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
            ),
        ]

        preflight_message = TimeSeriesRequest(
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
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_PREFLIGHT
                ),
            ),
            aggregations=aggregations,
            granularity_secs=granularity_secs,
        )

        message_to_non_downsampled_tier = TimeSeriesRequest(
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
            aggregations=aggregations,
            granularity_secs=granularity_secs,
        )

        preflight_response = EndpointTimeSeries().execute(preflight_message)
        non_downsampled_tier_response = EndpointTimeSeries().execute(
            message_to_non_downsampled_tier
        )

        if preflight_response.result_timeseries == []:
            sum_of_preflight_metric = 0.0
        else:
            sum_of_preflight_metric = (
                preflight_response.result_timeseries[0].data_points[0].data
            )

        assert (
            sum_of_preflight_metric
            < non_downsampled_tier_response.result_timeseries[0].data_points[0].data
            / 10
        )
        assert (
            preflight_response.meta.downsampled_storage_meta
            == DownsampledStorageMeta(
                tier=DownsampledStorageMeta.SelectedTier.SELECTED_TIER_64
            )
        )

    def test_best_effort_route_to_tier_64(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        granularity_secs = 3600
        query_duration = granularity_secs * 1
        store_spans_timeseries(
            BASE_TIME,
            1,
            query_duration,
            metrics=[DummyMetric("test_best_effort", get_value=lambda x: 1)],
        )

        aggregations = [
            AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_best_effort"),
                label="sum",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
            ),
        ]

        # sends a best effort request and a non-downsampled request to ensure their responses are different
        best_effort_downsample_message = TimeSeriesRequest(
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
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            aggregations=aggregations,
            granularity_secs=granularity_secs,
        )
        message_to_non_downsampled_tier = TimeSeriesRequest(
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
            aggregations=aggregations,
            granularity_secs=granularity_secs,
        )
        # this forces the query to route to tier 64. take a look at _get_target_tier to find out why
        with patch(
            "snuba.web.rpc.v1.resolvers.R_eap_items.storage_routing.routing_strategies.linear_bytes_scanned_storage_routing.LinearBytesScannedRoutingStrategy._get_query_bytes_scanned",
            return_value=20132659201,
        ):
            best_effort_response = EndpointTimeSeries().execute(
                best_effort_downsample_message
            )
            print(best_effort_response)
            non_downsampled_tier_response = EndpointTimeSeries().execute(
                message_to_non_downsampled_tier
            )

            best_effort_metric_sum = (
                best_effort_response.result_timeseries[0].data_points[0].data
            )

            # tier 1 sum should be 3600, so tier 64 sum should be around 3600 / 64 (give or take due to random sampling)
            non_downsampled_best_effort_metric_sum = (
                non_downsampled_tier_response.result_timeseries[0].data_points[0].data
            )
            assert (
                non_downsampled_best_effort_metric_sum / 200
                <= best_effort_metric_sum
                <= non_downsampled_best_effort_metric_sum / 16
            )

            assert (
                best_effort_response.meta.downsampled_storage_meta
                == DownsampledStorageMeta(
                    tier=DownsampledStorageMeta.SelectedTier.SELECTED_TIER_64
                )
            )

    def test_best_effort_end_to_end(self) -> None:
        granularity_secs = 3600
        query_duration = granularity_secs * 1
        store_spans_timeseries(
            BASE_TIME,
            1,
            query_duration,
            metrics=[DummyMetric("endtoend", get_value=lambda x: 1)],
        )

        best_effort_downsample_message = TimeSeriesRequest(
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
                downsampled_storage_config=DownsampledStorageConfig(
                    mode=DownsampledStorageConfig.MODE_BEST_EFFORT
                ),
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="endtoend"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(best_effort_downsample_message)
        assert (
            response.meta.downsampled_storage_meta.tier
            != DownsampledStorageMeta.SELECTED_TIER_UNSPECIFIED
        )


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
