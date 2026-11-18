from datetime import timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta, TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    HistogramMode,
    Reliability,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter, TraceItemFilter
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import BASE_TIME, gen_item_message

VALUE = AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.value")
METRIC_NAME = AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.metric_name")


def _row(metric_name: str, value: float, **attributes: AnyValue) -> bytes:
    return gen_item_message(
        start_timestamp=BASE_TIME + timedelta(minutes=1),
        type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
        remove_default_attributes=True,
        attributes={
            "sentry.metric_name": AnyValue(string_value=metric_name),
            "sentry.value": AnyValue(double_value=value),
            **attributes,
        },
    )


def _bucket(lower: float, upper: float, count: int, sum_share: float) -> bytes:
    return _row(
        "latency",
        (lower + upper) / 2,
        **{
            "sentry._internal.row_kind": AnyValue(string_value="bucket"),
            "sentry._internal.bucket_lower": AnyValue(double_value=lower),
            "sentry._internal.bucket_upper": AnyValue(double_value=upper),
            "sentry._internal.bucket_count": AnyValue(int_value=count),
            "sentry._internal.sum_share": AnyValue(double_value=sum_share),
        },
    )


def _exemplar(metric_name: str) -> bytes:
    return _row(
        metric_name,
        1000.0,
        **{"sentry._internal.row_kind": AnyValue(string_value="exemplar")},
    )


@pytest.fixture(autouse=False)
def setup_histograms(clickhouse_db: None, redis_db: None) -> None:
    write_raw_unprocessed_events(
        get_writable_storage(StorageKey("eap_items")),
        [
            # one OTLP histogram point with two buckets and an exemplar
            _bucket(0, 10, count=4, sum_share=20),
            _bucket(10, 20, count=6, sum_share=90),
            _exemplar("latency"),
            # one plain row, which counts as a single observation
            _row("latency", 30),
            _exemplar("only_exemplars"),
        ],
    )


def _meta() -> RequestMeta:
    return RequestMeta(
        project_ids=[1],
        organization_id=1,
        cogs_category="something",
        referrer="something",
        start_timestamp=Timestamp(seconds=int(BASE_TIME.timestamp())),
        end_timestamp=Timestamp(seconds=int((BASE_TIME + timedelta(hours=1)).timestamp())),
        trace_item_type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
    )


def _column(
    function: Function.ValueType,
    histogram_mode: HistogramMode.ValueType = HistogramMode.HISTOGRAM_MODE_BUCKETS,
) -> Column:
    return Column(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=function,
            key=VALUE,
            label=Function.Name(function),
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            histogram_mode=histogram_mode,
        ),
    )


def _table(columns: list[Column]) -> dict[str, dict[str, AttributeValue]]:
    response = EndpointTraceItemTable().execute(
        TraceItemTableRequest(
            meta=_meta(),
            columns=[Column(key=METRIC_NAME), *columns],
            group_by=[METRIC_NAME],
            order_by=[TraceItemTableRequest.OrderBy(column=Column(key=METRIC_NAME))],
            limit=10,
        )
    )
    names = [v.val_str for v in response.column_values[0].results]
    return {
        name: {column.attribute_name: column.results[i] for column in response.column_values[1:]}
        for i, name in enumerate(names)
    }


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestHistogramAggregation(BaseApiTest):
    def test_buckets(self, setup_histograms: None) -> None:
        functions = [
            Function.FUNCTION_COUNT,
            Function.FUNCTION_SUM,
            Function.FUNCTION_AVG,
            Function.FUNCTION_MIN,
            Function.FUNCTION_MAX,
            Function.FUNCTION_P50,
            Function.FUNCTION_P99,
        ]
        result = _table([_column(function) for function in functions])

        assert {k: v.val_double for k, v in result["latency"].items()} == {
            "FUNCTION_COUNT": 11,
            "FUNCTION_SUM": 140,
            "FUNCTION_AVG": pytest.approx(140 / 11),
            "FUNCTION_MIN": 0,
            "FUNCTION_MAX": 30,
            # rank 5.5 falls into [10, 20], 1.5 of its 6 observations in
            "FUNCTION_P50": pytest.approx(12.5),
            "FUNCTION_P99": 30,
        }
        assert result["only_exemplars"]["FUNCTION_P50"] == AttributeValue(is_null=True)

    def test_without_histogram_mode(self, setup_histograms: None) -> None:
        result = _table(
            [
                _column(Function.FUNCTION_COUNT, HistogramMode.HISTOGRAM_MODE_UNSPECIFIED),
                _column(Function.FUNCTION_MAX, HistogramMode.HISTOGRAM_MODE_NONE),
            ]
        )

        assert result["latency"]["FUNCTION_COUNT"].val_double == 4
        assert result["latency"]["FUNCTION_MAX"].val_double == 1000

    def test_time_series(self, setup_histograms: None) -> None:
        response = EndpointTimeSeries().execute(
            TimeSeriesRequest(
                meta=_meta(),
                filter=TraceItemFilter(
                    comparison_filter=ComparisonFilter(
                        key=METRIC_NAME,
                        op=ComparisonFilter.OP_EQUALS,
                        value=AttributeValue(val_str="latency"),
                    )
                ),
                aggregations=[
                    AttributeAggregation(
                        aggregate=Function.FUNCTION_P50,
                        key=VALUE,
                        label="p50",
                        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                        histogram_mode=HistogramMode.HISTOGRAM_MODE_BUCKETS,
                    ),
                ],
                granularity_secs=3600,
            )
        )

        [data_point] = response.result_timeseries[0].data_points
        assert data_point.data == pytest.approx(12.5)
        # exemplar rows are not samples
        assert data_point.sample_count == 3
        assert data_point.reliability == Reliability.RELIABILITY_LOW

    def test_unsupported_function(self) -> None:
        with pytest.raises(BadSnubaRPCRequestException):
            _table([_column(Function.FUNCTION_UNIQ)])
