from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Callable

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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

BASE_TIME = datetime.now(tz=UTC).replace(
    hour=8,
    minute=0,
    second=0,
    microsecond=0,
) - timedelta(hours=24)


SecsFromSeriesStart = int


@dataclass
class DummyMetric:
    name: str
    get_value: Callable[[SecsFromSeriesStart], float]


def store_timeseries(
    start_datetime: datetime,
    period_secs: int,
    len_secs: int,
    metrics: list[DummyMetric],
    tags: dict[str, str] | None = None,
    server_sample_rate: float | Callable[[SecsFromSeriesStart], float] = 1.0,
) -> None:
    tags = tags or {}
    messages = []
    for secs in range(0, len_secs, period_secs):
        dt = start_datetime + timedelta(seconds=secs)
        numbers = {
            m.name: AnyValue(double_value=float(m.get_value(secs))) for m in metrics
        }
        if callable(server_sample_rate):
            real_server_sample_rate = server_sample_rate(secs)
        else:
            real_server_sample_rate = server_sample_rate
        messages.append(
            gen_item_message(
                start_timestamp=dt,
                attributes=numbers,
                server_sample_rate=real_server_sample_rate,
            ),
        )
    items_storage = get_storage(StorageKey("eap_items"))
    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


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
            server_sample_rate=1.0,  # 100% sample rate should result in reliable extrapolation
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
                        reliability=Reliability.RELIABILITY_HIGH,
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
            server_sample_rate=0.0001,
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
            metrics=[DummyMetric("test_metric", get_value=lambda x: 55 - (x % 120))],
            server_sample_rate=0.0001,  # 0.01% sample rate should be unreliable
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
        expected_sum = sum([(55 - (x % 120)) / 0.0001 for x in range(0, 120)])
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

    def test_avg_empty(self) -> None:
        """
        Asserts that the divisions in the confidence computation for avg do not
        result in errors or low confidence - we expect an empty result set to be
        returned.
        """

        granularity_secs = 120
        query_duration = 3600

        store_timeseries(
            BASE_TIME,
            query_duration // 2,
            query_duration,
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
        expected_points = [
            (
                DataPoint(
                    data=1,
                    data_present=True,
                    reliability=Reliability.RELIABILITY_HIGH,
                    avg_sampling_rate=1.0,
                    sample_count=1,
                )
                if secs % (query_duration // 2) == 0
                else DataPoint(data_present=False)
            )
            for secs in range(0, query_duration, granularity_secs)
        ]

        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            TimeSeries(
                label="avg(test_metric)",
                buckets=expected_buckets,
                data_points=expected_points,
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
            server_sample_rate=0.0001,  # 0.01% sample rate should be unreliable
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
            server_sample_rate=0.0001,  # 0.01% sample rate should be unreliable
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
            server_sample_rate=lambda s: 0.01 if (s / 60) % 2 == 0 else 1.0,
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1],
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
                        # 2 events (1 with 1% sampling rate and 1 with 100% sampling rate)
                        data=1 / 0.01 + 1,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_LOW,
                        # weighted average = (1 + 1)/(1/0.01 + 1) = 2/101
                        avg_sampling_rate=2 / 101,
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
            server_sample_rate=lambda s: sample_rate,
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

    def test_formula_reliability(self) -> None:
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
            server_sample_rate=lambda s: sample_rate,
        )
        expr1 = Expression(
            aggregation=AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="my_test_metric"),
                label="expr1 + expr2.left",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ),
            label="expr1 + expr2.left",
        )
        expr2 = Expression(
            aggregation=AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="my_test_metric"),
                label="expr1 + expr2.right",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ),
            label="expr1 + expr2.right",
        )
        expr3 = Expression(
            aggregation=AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="my_test_metric"),
                label="expr3",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ),
            label="expr3",
        )
        expr4 = Expression(
            aggregation=AttributeAggregation(
                aggregate=Function.FUNCTION_SUM,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="my_test_metric"),
                label="expr4",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ),
            label="expr4",
        )
        myform2 = Expression(
            formula=Expression.BinaryFormula(
                op=Expression.BinaryFormula.OP_ADD,
                left=expr3,
                right=expr4,
            ),
            label="myform2",
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
                        left=expr1,
                        right=expr2,
                    ),
                    label="expr1 + expr2",
                ),
                myform2,
                expr3,
            ],
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_expr1 = (granularity_secs * metric_value) / sample_rate
        expected_expr2 = (granularity_secs * metric_value) / sample_rate
        expected_sum_plus_sum = expected_expr1 + expected_expr2
        tmp = sorted(response.result_timeseries, key=lambda x: x.label)
        tmp2 = [
            TimeSeries(
                label="expr1",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=expected_expr1,
                        data_present=True,
                        avg_sampling_rate=sample_rate,
                        sample_count=120,
                        reliability=Reliability.RELIABILITY_HIGH,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="expr1 + expr2",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=expected_sum_plus_sum,
                        data_present=True,
                        reliability=Reliability.RELIABILITY_HIGH,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="expr2",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=expected_expr2,
                        data_present=True,
                        avg_sampling_rate=sample_rate,
                        sample_count=120,
                        reliability=Reliability.RELIABILITY_HIGH,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]
        assert tmp == tmp2
