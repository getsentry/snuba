from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import chain
from typing import Any, Callable
from unittest.mock import MagicMock, call, patch

import pytest
from clickhouse_driver.errors import ServerException
from google.protobuf.json_format import ParseDict
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
    AttributeKeyExpression,
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
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue

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
from tests.web.rpc.v1.test_utils import gen_item_message

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
    attributes: dict[str, AnyValue] = {},
) -> None:
    messages = []
    for secs in range(0, len_secs, period_secs):
        dt = start_datetime + timedelta(seconds=secs)
        a = attributes | {m.name: AnyValue(double_value=m.get_value(secs)) for m in metrics}
        messages.append(gen_item_message(dt, a))
    items_storage = get_storage(StorageKey("eap_items"))

    write_raw_unprocessed_events(items_storage, messages)  # type: ignore


@pytest.mark.eap
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
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                    label="p50",
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P95,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                    label="p90",
                ),
            ],
            granularity_secs=60,
        )
        response = self.app.post("/rpc/EndpointTimeSeries/v1", data=message.SerializeToString())
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
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                    label="count",
                ),
            ],
            granularity_secs=60,
        )
        response = self.app.post("/rpc/EndpointTimeSeries/v1", data=message.SerializeToString())
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

        test_metric_attribute_key = AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric")
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
                DataPoint(data=150, data_present=True, sample_count=150)
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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

    def test_any(self) -> None:
        # store a test metric with a value of 1, every second of one hour
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_ANY,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                    label="any",
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
        # any() returns any value from the group - since all values are 1, we expect 1
        assert response.result_timeseries == [
            TimeSeries(
                label="any",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=1, data_present=True, sample_count=300)
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
            attributes={
                "consumer_group": AnyValue(string_value="a"),
                "environment": AnyValue(string_value="prod"),
            },
        )
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 10)],
            attributes={
                "consumer_group": AnyValue(string_value="z"),
                "environment": AnyValue(string_value="prod"),
            },
        )
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 100)],
            attributes={
                "consumer_group": AnyValue(string_value="z"),
                "environment": AnyValue(string_value="dev"),
            },
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
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs) for secs in range(0, 60 * 30, 300)
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
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs) for secs in range(0, 60 * 30, 300)
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            aggregations=[
                AttributeAggregation(
                    aggregate=Function.FUNCTION_SUM,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sparse_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_AVG,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sparse_metric"),
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
            attributes={"customer": AnyValue(string_value="bob")},
        )

        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 999)],
            attributes={"customer": AnyValue(string_value="alice")},
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
                                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="customer"),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="bob"),
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="customer"),
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
            attributes={"customer": AnyValue(string_value="bOb")},
        )

        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 999)],
            attributes={"customer": AnyValue(string_value="aLiCe")},
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
                                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="customer"),
                                op=ComparisonFilter.OP_EQUALS,
                                value=AttributeValue(val_str="BoB"),
                                ignore_case=True,
                            )
                        ),
                        TraceItemFilter(
                            comparison_filter=ComparisonFilter(
                                key=AttributeKey(type=AttributeKey.TYPE_STRING, name="customer"),
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
            attributes={"customer": AnyValue(string_value="bob")},
        )
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp()) + query_duration),
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
            for secs in range(0, query_duration - query_offset + granularity_secs, granularity_secs)
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration + 1)),
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
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="non_existent_metric"),
                    label="sum",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                ),
            ],
            granularity_secs=300,
        )

        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs) for secs in range(0, 60 * 30, 300)
        ]

        assert response.result_timeseries == [
            TimeSeries(
                label="sum",
                buckets=expected_buckets,
                data_points=[DataPoint(data_present=False) for _ in range(len(expected_buckets))],
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
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                    label="p50",
                ),
                AttributeAggregation(
                    aggregate=Function.FUNCTION_P95,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="sentry.duration"),
                    label="p90",
                ),
            ],
            granularity_secs=60,
        )

        metrics_mock = MagicMock()
        monkeypatch.setattr(RPCEndpoint, "metrics", property(lambda x: metrics_mock))
        with (
            patch(
                "clickhouse_driver.client.Client.execute",
                side_effect=ServerException(
                    "DB::Exception: Received from snuba-events-analytics-platform-1-1:1111. DB::Exception: Memory limit (for query) exceeded: would use 1.11GiB (attempt to allocate chunk of 111111 bytes), maximum: 1.11 GiB. Blahblahblahblahblahblahblah",
                    code=241,
                ),
            ),
            patch("snuba.web.rpc.sentry_sdk.capture_exception") as sentry_sdk_mock,
        ):
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                                label="sum",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_AVG,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
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
                DataPoint(data=300, data_present=True, sample_count=300)
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                        label="sum(test_metric)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label="sum(test_metric)",
                ),
            ],
            group_by=[
                AttributeKey(
                    type=AttributeKey.TYPE_STRING,
                    name="sentry.raw_description",
                ),
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
            group_by_attributes={
                "sentry.raw_description": "/api/0/relays/projectconfigs/",
            },
            buckets=expected_buckets,
            data_points=[
                DataPoint(data=300, data_present=True, sample_count=300)
                for _ in range(len(expected_buckets))
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [expected_timeseries]

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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration_secs)),
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
                    sample_count=600,
                ),
                DataPoint(
                    data=2,
                    data_present=True,
                    sample_count=600,
                ),
                DataPoint(
                    data=2,
                    data_present=True,
                    sample_count=600,
                ),
                DataPoint(data=-1.0, data_present=True, sample_count=600),
                DataPoint(data=-1.0, data_present=True, sample_count=600),
                DataPoint(data=-1.0, data_present=True, sample_count=600),
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
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
                DataPoint(data=300, data_present=True, sample_count=granularity_secs)
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
                # literal division should have no samples
                DataPoint(data=0.5, data_present=True, sample_count=0)
                for _ in range(len(expected_buckets))
            ],
        )
        assert sorted(response.result_timeseries, key=lambda x: x.label) == [
            expected_literal_timeseries,
            expected_formula_timeseries,
        ]

    @pytest.mark.xfail(reason="Outcomes based strategy does not care about query mode")
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
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_preflight_metric"),
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
            sum_of_preflight_metric = preflight_response.result_timeseries[0].data_points[0].data

        assert (
            sum_of_preflight_metric
            < non_downsampled_tier_response.result_timeseries[0].data_points[0].data / 10
        )
        assert preflight_response.meta.downsampled_storage_meta == DownsampledStorageMeta(
            can_go_to_higher_accuracy_tier=True,
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
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
        EndpointTimeSeries().execute(best_effort_downsample_message)

    def test_duplicate_top_level_labels(self) -> None:
        """
        This test ensures that duplicate labels in top level expressions
        raises exception
        """
        granularity_secs = 3600
        query_duration = granularity_secs * 1
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric1"),
                    ),
                    label="mylabel",
                ),
                Expression(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric2"),
                    ),
                    label="mylabel",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Duplicate expression label: mylabel"
        ):
            EndpointTimeSeries().execute(message)

    def test_duplicate_labels_inner(self) -> None:
        """
        This test ensures that duplicate labels across different expressions
        doesnt cause incorrect behavior
        """
        granularity_secs = 30
        query_duration = granularity_secs * 4
        metric1_value = 3
        metric2_value = 7
        store_spans_timeseries(
            BASE_TIME,
            1,
            query_duration,
            metrics=[
                DummyMetric("metric1", get_value=lambda x: metric1_value),
                DummyMetric("metric2", get_value=lambda x: metric2_value),
            ],
        )
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            # this does:
            # plus(metric1 AS part1, metric1 AS part2)
            # plus(metric2 AS part1, metric2 AS part2)
            # the 2 different expressions share labels for the inner parts of the formula
            # (part1, part2 are the duplicated labels)
            # previously this would causes incorrect behavior, this test ensures that it doesn't
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric1"),
                                label="part1",
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric1"),
                                label="part2",
                            )
                        ),
                    ),
                    label="metric1",
                ),
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_ADD,
                        left=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric2"),
                                label="part1",
                                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                            )
                        ),
                        right=Expression(
                            aggregation=AttributeAggregation(
                                aggregate=Function.FUNCTION_SUM,
                                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="metric2"),
                                label="part2",
                            )
                        ),
                    ),
                    label="metric2",
                ),
            ],
            granularity_secs=granularity_secs,
        )
        res = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]
        expected_timeseries = [
            TimeSeries(
                label="metric1",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=granularity_secs * (metric1_value * 2),
                        data_present=True,
                        sample_count=granularity_secs,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
            TimeSeries(
                label="metric2",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(
                        data=granularity_secs * (metric2_value * 2),
                        data_present=True,
                        sample_count=granularity_secs,
                    )
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]
        assert sorted(res.result_timeseries, key=lambda e: e.label) == expected_timeseries

    def test_agg_label_diff_from_expr_label(self) -> None:
        """
        ensure that when the label of the aggregate differs from the label of the expression,
        it still works
        """
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
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test_metric"),
                        label="otherlabel",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label="label",
                ),
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
                label="label",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_filter_on_timestamp_string(self) -> None:
        # store a a test metric with a value of 1, every second of one hour
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("test_metric", get_value=lambda x: 1)],
        )

        query = {
            "expressions": [
                {
                    "conditionalAggregation": {
                        "aggregate": "FUNCTION_SUM",
                        "extrapolationMode": "EXTRAPOLATION_MODE_SAMPLE_WEIGHTED",
                        "key": {"name": "test_metric", "type": "TYPE_DOUBLE"},
                        "label": "sum(test_metric)",
                    },
                    "label": "sum(test_metric)",
                }
            ],
            "filter": {
                "comparisonFilter": {
                    "key": {
                        "name": "sentry.timestamp",
                        "type": "TYPE_STRING",
                    },
                    "op": "OP_GREATER_THAN_OR_EQUALS",
                    "value": {"valStr": (BASE_TIME + timedelta(minutes=30)).isoformat()},
                }
            },
            "granularitySecs": "60",
            "meta": {
                "downsampledStorageConfig": {"mode": "MODE_NORMAL"},
                "endTimestamp": (BASE_TIME + timedelta(hours=1)).isoformat(),
                "organizationId": "1",
                "projectIds": [
                    "1",
                ],
                "referrer": "api.dashboards.widget.area-chart",
                "requestId": "4da24e8f-b4a0-413f-835a-01dc3bf063d8",
                "startTimestamp": BASE_TIME.isoformat(),
                "traceItemType": "TRACE_ITEM_TYPE_SPAN",
            },
        }

        message = ParseDict(query, TimeSeriesRequest())
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTimeSeries().execute(message)

    def test_coalesce_attributes(self) -> None:
        granularity_secs = 300
        query_duration = 60 * 30

        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[
                DummyMetric(
                    "ai.total_tokens.used",
                    get_value=lambda x: 1,
                ),
            ],
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    label="label",
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT,
                            name="gen_ai.usage.total_tokens",
                        ),
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
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
                label="label",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=300, data_present=True, sample_count=300)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    def test_filter_coalesce_attributes(self) -> None:
        granularity_secs = 300
        query_duration = 60 * 30

        # does not match the `url.path = "a"` filter
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("gen_ai.usage.total_tokens", get_value=lambda x: 1)],
        )

        # matches the `url.path = "a"` filter
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("gen_ai.usage.total_tokens", get_value=lambda x: 1)],
            attributes={"url.path": AnyValue(string_value="a")},
        )

        # matches the `url.path = "a"` filter because it's coalesced using `http.target`
        store_spans_timeseries(
            BASE_TIME,
            1,
            3600,
            metrics=[DummyMetric("gen_ai.usage.total_tokens", get_value=lambda x: 1)],
            attributes={"http.target": AnyValue(string_value="a")},
        )

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    label="label",
                    aggregation=AttributeAggregation(
                        aggregate=Function.FUNCTION_SUM,
                        key=AttributeKey(
                            type=AttributeKey.TYPE_INT,
                            name="gen_ai.usage.total_tokens",
                        ),
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                ),
            ],
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="url.path"),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="a"),
                )
            ),
            granularity_secs=granularity_secs,
        )
        response = EndpointTimeSeries().execute(message)
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]

        assert response.result_timeseries == [
            TimeSeries(
                label="label",
                buckets=expected_buckets,
                data_points=[
                    DataPoint(data=600, data_present=True, sample_count=600)
                    for _ in range(len(expected_buckets))
                ],
            ),
        ]

    @pytest.mark.clickhouse_db
    def test_crash_free_user_rate_with_array_attributes(self) -> None:
        """Test FUNCTION_UNIQ on TYPE_ARRAY attributes for crash-free user rate."""
        granularity_secs = 3600
        query_duration = 3600

        def make_array(*names: str) -> AnyValue:
            return AnyValue(
                array_value=ArrayValue(values=[AnyValue(string_value=n) for n in names])
            )

        messages = []
        # 3 crashed sessions with overlapping user arrays
        # Unique crashed users: alice, bob, charlie, dave = 4
        crashed_users = [
            ["alice", "bob"],
            ["bob", "charlie"],
            ["alice", "dave"],
        ]
        for i, users in enumerate(crashed_users):
            dt = BASE_TIME + timedelta(seconds=i * 10)
            messages.append(
                gen_item_message(
                    dt,
                    {
                        "user_ids": make_array(*users),
                        "is_crashed": AnyValue(string_value="true"),
                    },
                    type=TraceItemType.TRACE_ITEM_TYPE_USER_SESSION,
                )
            )

        # 7 healthy sessions covering alice..jane
        # Total unique users across all 10 sessions: 10 (alice..jane)
        healthy_users = [
            ["alice", "bob", "charlie"],
            ["dave", "eve"],
            ["frank", "grace"],
            ["henry", "irene"],
            ["jane"],
            ["alice", "eve", "jane"],
            ["bob", "frank"],
        ]
        for i, users in enumerate(healthy_users):
            dt = BASE_TIME + timedelta(seconds=(i + 3) * 10)
            messages.append(
                gen_item_message(
                    dt,
                    {
                        "user_ids": make_array(*users),
                        "is_crashed": AnyValue(string_value="false"),
                    },
                    type=TraceItemType.TRACE_ITEM_TYPE_USER_SESSION,
                )
            )

        items_storage = get_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(items_storage, messages)  # type: ignore

        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp()) + query_duration),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_USER_SESSION,
            ),
            expressions=[
                Expression(
                    formula=Expression.BinaryFormula(
                        op=Expression.BinaryFormula.OP_SUBTRACT,
                        left=Expression(literal=Literal(val_double=1.0)),
                        right=Expression(
                            formula=Expression.BinaryFormula(
                                op=Expression.BinaryFormula.OP_DIVIDE,
                                left=Expression(
                                    conditional_aggregation=AttributeConditionalAggregation(
                                        aggregate=Function.FUNCTION_UNIQ,
                                        key=AttributeKey(
                                            type=AttributeKey.TYPE_ARRAY,
                                            name="user_ids",
                                        ),
                                        filter=TraceItemFilter(
                                            comparison_filter=ComparisonFilter(
                                                key=AttributeKey(
                                                    type=AttributeKey.TYPE_STRING,
                                                    name="is_crashed",
                                                ),
                                                op=ComparisonFilter.OP_EQUALS,
                                                value=AttributeValue(val_str="true"),
                                            )
                                        ),
                                        label="crashed_users",
                                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                    ),
                                ),
                                right=Expression(
                                    conditional_aggregation=AttributeConditionalAggregation(
                                        aggregate=Function.FUNCTION_UNIQ,
                                        key=AttributeKey(
                                            type=AttributeKey.TYPE_ARRAY,
                                            name="user_ids",
                                        ),
                                        label="all_users",
                                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    label="crash_free_user_rate",
                ),
            ],
            granularity_secs=granularity_secs,
        )

        response = EndpointTimeSeries().execute(message)
        ts = response.result_timeseries[0]
        assert ts.label == "crash_free_user_rate"
        assert ts.data_points[0].data == pytest.approx(0.6)
        assert ts.data_points[0].data_present is True

    def test_muliply_attribute_aggregation(self) -> None:
        """
        implementation details:
        * use store_spans_timeseries to store two different metrics with 2 different attributes:
            * the first attr is game_size double and the second attr is game_size_unit_mult and is a number with different value
            * first create some spans with the attributes: game_size = 1 to 10 (deterministic) and game_size_unit_mult = 10^9 (GB)
            * then create some spans with the attributes: game_size = 500 to 850 (deterministic) and game_size_unit_mult = 10^6 (MB)
            * then create a variable expected_avg to be the avg of (game_size * game_size_unit_mult) for each span
        * create a time series request for avg(game_size * game_size_unit_mult)
        * execute the request and verify that the result is equal to expected_avg
        * you dont have to actually run the test bc this is red-green TDD
        """
        granularity_secs = 300
        query_duration = 60 * 30

        # our data points: [1gb, 2gb, ... 500mb, 501mb, ... 850mb]
        data_points_gb = range(1, 10 + 1)
        data_points_mb = range(500, 850 + 1)
        # store them using the attributes game_size and game_size_unit_mult
        store_spans_timeseries(
            BASE_TIME,
            1,  # one span per second
            len(data_points_gb),
            metrics=[
                DummyMetric("game_size", get_value=lambda x: data_points_gb[x]),
            ],
            attributes={
                "game_size_unit_mult": AnyValue(int_value=10**9),
            },
        )
        store_spans_timeseries(
            BASE_TIME + timedelta(seconds=len(data_points_gb)),
            1,  # one span per second
            len(data_points_mb),
            metrics=[
                DummyMetric("game_size", get_value=lambda x: data_points_mb[x]),
            ],
            attributes={
                "game_size_unit_mult": AnyValue(int_value=10**6),
            },
        )
        # figure out the expected value for avg(game_size * game_size_unit_mult) timeseries
        data_points_bytes = list(
            chain(
                map(lambda x: x * 10**9, data_points_gb), map(lambda x: x * 10**6, data_points_mb)
            )
        )

        # query for avg(game_size * game_size_unit_mult)
        message = TimeSeriesRequest(
            meta=RequestMeta(
                project_ids=[1, 2, 3],
                organization_id=1,
                cogs_category="something",
                referrer="something",
                start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
                end_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp() + query_duration)),
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            expressions=[
                Expression(
                    conditional_aggregation=AttributeConditionalAggregation(
                        aggregate=Function.FUNCTION_AVG,
                        expression=AttributeKeyExpression(
                            formula=AttributeKeyExpression.Formula(
                                op=AttributeKeyExpression.OP_MULT,
                                left=AttributeKeyExpression(
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_DOUBLE, name="game_size"
                                    ),
                                ),
                                right=AttributeKeyExpression(
                                    key=AttributeKey(
                                        type=AttributeKey.TYPE_INT, name="game_size_unit_mult"
                                    ),
                                ),
                            )
                        ),
                        label="avg(game_size * game_size_unit_mult)",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
                    ),
                    label="avg(game_size * game_size_unit_mult)",
                ),
            ],
            granularity_secs=granularity_secs,
        )

        response = EndpointTimeSeries().execute(message)

        # Calculate expected buckets
        expected_buckets = [
            Timestamp(seconds=int(BASE_TIME.timestamp()) + secs)
            for secs in range(0, query_duration, granularity_secs)
        ]

        # Calculate average for each bucket using the data_points_bytes
        expected_data_points = []
        for bucket_start_secs in range(0, query_duration, granularity_secs):
            bucket_end_secs = bucket_start_secs + granularity_secs
            # Get data points that fall in this bucket
            bucket_values = data_points_bytes[
                bucket_start_secs : min(bucket_end_secs, len(data_points_bytes))
            ]

            if bucket_values:
                avg_value = sum(bucket_values) / len(bucket_values)
                expected_data_points.append(
                    DataPoint(data=avg_value, data_present=True, sample_count=len(bucket_values))
                )
            else:
                # No data in this bucket
                expected_data_points.append(DataPoint(data=0, data_present=False, sample_count=0))

        assert response.result_timeseries == [
            TimeSeries(
                label="avg(game_size * game_size_unit_mult)",
                buckets=expected_buckets,
                data_points=expected_data_points,
            )
        ]


class TestUtils:
    @pytest.mark.redis_db
    @pytest.mark.clickhouse_db
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
            (BASE_TIME, BASE_TIME + timedelta(hours=24 * 3), 15),
            (BASE_TIME, BASE_TIME + timedelta(hours=1), 0),
            (BASE_TIME + timedelta(hours=1), BASE_TIME, 0),
            (BASE_TIME, BASE_TIME + timedelta(hours=1), 3 * 3600),
        ],
    )
    def test_bad_granularity(self, start_ts: datetime, end_ts: datetime, granularity: int) -> None:
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
