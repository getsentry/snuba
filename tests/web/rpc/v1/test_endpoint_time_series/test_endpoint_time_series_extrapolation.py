import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, MutableMapping

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import (
    DataPoint,
    Expression,
    TimeSeries,
    TimeSeriesRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
    Reliability,
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
    measurements: dict[str, dict[str, float]],
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
        measurements_dict = {m.name: {"value": m.get_value(secs)} for m in measurements}
        messages.append(gen_message(dt, tags, numerical_attributes, measurements_dict))
    spans_storage = get_storage(StorageKey("eap_spans"))
    write_raw_unprocessed_events(spans_storage, messages)  # type: ignore


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestTimeSeriesApiWithExtrapolation(BaseApiTest):
    def test_aggregations_reliable(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 50)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate", get_value=lambda s: 1
                )  # 100% sample rate should result in reliable extrapolation
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="count(test_metric)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum(test_metric)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="avg(test_metric)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P50,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="p50(test_metric)",
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
                label="avg(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=50,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_HIGH,
                        avg_sampling_rate=1,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="count(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=120,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_HIGH,
                        avg_sampling_rate=1,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="p50(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=50,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_HIGH,
                        avg_sampling_rate=1,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="sum(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=120 * 50,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_HIGH,
                        avg_sampling_rate=1,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_confidence_interval_zero_estimate(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 0)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate", get_value=lambda s: 1
                )  # 100% sample rate should result in reliable extrapolation
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum(test_metric)",
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
        print(sorted(response.result_timeseries, key=lambda x: x.label))
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="sum(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=0,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=1,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            )
        ]

    def test_confidence_interval_no_samples(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 0)],
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P50,
                    key=AttributeKey(
                        type=AttributeKey.TYPE_FLOAT, name="test_metric2"
                    ),  # non-existent metric
                    label="p50(test_metric2)",
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
                label="p50(test_metric2)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data_present=False) for _ in range(len(expected_buckets))
                ],
            )
        ]

    def test_count_unreliable(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            # accuracy of counts depends on sample count and heterogenity of
            # sample rates. In this case, we artificially reduce the number of
            # samples to drive up the relative error.
            10,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            measurements=[
                DummyMeasurement("client_sample_rate", get_value=lambda s: 0.0001)
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="count(test_metric)",
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
                label="count(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=12 / 0.0001,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=0.0001,
                        sample_count=12,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_sum_unreliable(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: (x % 120) - 55)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate",
                    get_value=lambda s: 0.0001,  # 0.01% sample rate should be unreliable
                )
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="sum(test_metric)",
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
        expected_sum = sum([((x % 120) - 55) / 0.0001 for x in range(0, 120)])
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="sum(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=expected_sum,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=0.0001,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_avg_unreliable(self) -> None:
        # store a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            # for each time interval we distribute the values from -55 to 64 to keep the avg close to 0
            metrics=[DummyMetric("test_metric", get_value=lambda x: (x % 120) - 55)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate",
                    get_value=lambda s: 0.0001,  # 0.01% sample rate should be unreliable
                )
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="avg(test_metric)",
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
                label="avg(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=4.5,  # (-55 + -54 + ... + 64) / 120 = 4.5
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=0.0001,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_percentile_unreliable(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: (x % 120) - 85)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate",
                    get_value=lambda s: 0.0001,  # 0.01% sample rate should be unreliable
                )
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P75,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="p75(test_metric)",
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
                label="p75(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=4.5,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=0.0001,
                        sample_count=120,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_average_sampling_rate(self) -> None:
        granularity_secs = 120
        query_duration = 3600
        store_timeseries(
            BASE_TIME,
            60,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
            measurements=[
                DummyMeasurement(
                    # for each time bucket we store an event with 1% sampling rate and 100% sampling rate
                    "client_sample_rate",
                    get_value=lambda s: 0.01 if (s / 60) % 2 == 0 else 1,
                )
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
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_COUNT,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="count(test_metric)",
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
                label="count(test_metric)",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=1 / 0.01
                        + 1,  # 2 events (1 with 1% sampling rate and 1 with 100% sampling rate)
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        avg_sampling_rate=2
                        / 101,  # weighted average = (1 + 1)/(1/0.01 + 1) = 2/101
                        sample_count=2,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_formula(self) -> None:
        # store a a test metric with a value of 1, every second for an hour
        granularity_secs = 120
        query_duration = 3600
        sample_rate = 0.5
        metric_value = 10
        store_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("my_test_metric", get_value=lambda x: metric_value)],
            measurements=[
                DummyMeasurement(
                    "client_sample_rate",
                    get_value=lambda s: sample_rate,
                )
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
                                    type=AttributeKey.TYPE_FLOAT, name="my_test_metric"
                                ),
                                label="sum(test_metric)",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(
                                    type=AttributeKey.TYPE_FLOAT, name="my_test_metric"
                                ),
                                label="sum(test_metric)",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                            )
                        ),
                    ),
                    label="sum + sum",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_sum_plus_sum = (granularity_secs * metric_value * 2) / sample_rate
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="sum + sum",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=expected_sum_plus_sum,
                        data_present=True,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]
