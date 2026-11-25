from datetime import timedelta

import pytest
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import Expression, TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    Reliability,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    OrFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, ArrayValue

from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.common.histogram_aggregation import is_histogram_aggregation
from snuba.web.rpc.v1.endpoint_time_series import EndpointTimeSeries
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.base import BaseApiTest
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import BASE_TIME, create_request_meta, gen_item_message

VALUE = AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="sentry.value")
METRIC_NAME = AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.metric_name")
IS_HISTOGRAM = TraceItemFilter(
    comparison_filter=ComparisonFilter(
        key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.metric_type"),
        op=ComparisonFilter.OP_EQUALS,
        value=AttributeValue(val_str="histogram"),
    )
)
META = create_request_meta(
    BASE_TIME, BASE_TIME + timedelta(hours=1), TraceItemType.TRACE_ITEM_TYPE_METRIC
)
AT_LEAST_250 = TraceItemFilter(
    comparison_filter=ComparisonFilter(
        key=VALUE,
        op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
        value=AttributeValue(val_double=250),
    )
)


def _row(
    metric_name: str,
    value: float,
    attributes: dict[str, AnyValue] | None = None,
    server_sample_rate: float = 1.0,
) -> bytes:
    return gen_item_message(
        start_timestamp=BASE_TIME + timedelta(minutes=1),
        type=TraceItemType.TRACE_ITEM_TYPE_METRIC,
        remove_default_attributes=True,
        server_sample_rate=server_sample_rate,
        attributes={
            "sentry.metric_name": AnyValue(string_value=metric_name),
            "sentry.metric_type": AnyValue(string_value="histogram"),
            "sentry.value": AnyValue(double_value=value),
            **(attributes or {}),
        },
    )


def _array(values: list[float] | list[int]) -> AnyValue:
    return AnyValue(
        array_value=ArrayValue(
            values=[
                AnyValue(int_value=v) if isinstance(v, int) else AnyValue(double_value=v)
                for v in values
            ]
        )
    )


def _histogram(
    metric_name: str,
    buckets: list[tuple[float, float, int]],
    total: float | None = None,
    point_min_max: tuple[float, float] | None = None,
    server_sample_rate: float = 1.0,
) -> bytes:
    """A histogram point as Relay writes it, with buckets of (lower, upper, count)."""
    # each bucket's share of the sum is proportional to count * midpoint
    sum_shares = [count * (lower + upper) / 2 for lower, upper, count in buckets]
    if total is not None:
        sum_shares = [share * total / sum(sum_shares) for share in sum_shares]
    count = sum(count for _, _, count in buckets)
    attributes = {
        "sentry._internal.bucket_lowers": _array([float(b[0]) for b in buckets]),
        "sentry._internal.bucket_uppers": _array([float(b[1]) for b in buckets]),
        "sentry._internal.bucket_counts": _array([b[2] for b in buckets]),
        "sentry._internal.bucket_sum_shares": _array(sum_shares),
        "sentry._internal.histogram_count": AnyValue(int_value=count),
        "sentry._internal.histogram_sum": AnyValue(double_value=sum(sum_shares)),
    }
    if point_min_max is not None:
        attributes["sentry._internal.histogram_min"] = AnyValue(double_value=point_min_max[0])
        attributes["sentry._internal.histogram_max"] = AnyValue(double_value=point_min_max[1])
    return _row(metric_name, sum(sum_shares) / count, attributes, server_sample_rate)


def _exemplar(metric_name: str) -> bytes:
    return _row(
        metric_name,
        1000.0,
        {"sentry.metric_type": AnyValue(string_value="histogram_exemplar")},
    )


@pytest.fixture(autouse=False)
def setup_histograms(eap: None, redis_db: None) -> None:
    write_raw_unprocessed_events(
        get_writable_storage(StorageKey("eap_items")),
        [
            # the example point from the design doc, sum 190000, with the outer buckets
            # clamped to the point's min and max like Relay does
            _histogram(
                "latency",
                [(102.4, 250, 900), (250, 500, 80), (500, 712.9, 20)],
                total=190000,
                point_min_max=(102.4, 712.9),
            ),
            _exemplar("latency"),
            # two bucket layouts for one metric
            _histogram("mixed", [(25, 50, 10), (50, 75, 10), (75, 100, 10)]),
            _histogram("mixed", [(25, 50, 10), (50, 100, 10)]),
            # rows without buckets are one observation each
            _row("plain", 10),
            _row("plain", 20),
            _histogram("sampled", [(0, 10, 3)], server_sample_rate=0.5),
            # most observations in a low bucket, a tail far above it, nothing in between
            _histogram("gap", [(0.4, 5, 9400), (2500, 4870, 600)]),
            # arrays of different lengths, read as a row without buckets
            _row(
                "mismatched",
                7,
                {
                    "sentry._internal.bucket_lowers": _array([0.0]),
                    "sentry._internal.bucket_uppers": _array([10.0, 20.0]),
                    "sentry._internal.bucket_counts": _array([5, 5]),
                    "sentry._internal.bucket_sum_shares": _array([25.0, 75.0]),
                },
            ),
        ],
    )


def _histogram_filter(filter: TraceItemFilter | None = None) -> TraceItemFilter:
    if filter is None:
        return IS_HISTOGRAM
    return TraceItemFilter(and_filter=AndFilter(filters=[IS_HISTOGRAM, filter]))


def _column(
    function: Function.ValueType,
    filter: TraceItemFilter | None = None,
    histogram: bool = True,
) -> Column:
    return Column(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=function,
            key=VALUE,
            label=Function.Name(function),
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            filter=_histogram_filter(filter) if histogram else filter,
        ),
    )


def _histogram_count() -> Expression:
    return Expression(
        conditional_aggregation=AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_COUNT,
            key=VALUE,
            label="count",
            extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            filter=IS_HISTOGRAM,
        ),
        label="count",
    )


def _table(
    columns: list[Column], filter: TraceItemFilter | None = None
) -> dict[str, dict[str, float]]:
    """Runs a table query grouped by metric name, returning {metric_name: {label: value}}."""
    response = EndpointTraceItemTable().execute(
        TraceItemTableRequest(
            meta=META,
            filter=filter,
            columns=[Column(key=METRIC_NAME), *columns],
            group_by=[METRIC_NAME],
            order_by=[TraceItemTableRequest.OrderBy(column=Column(key=METRIC_NAME))],
            limit=10,
        )
    )
    names = [v.val_str for v in response.column_values[0].results]
    return {
        name: {
            column.attribute_name: column.results[i].val_double
            for column in response.column_values[1:]
        }
        for i, name in enumerate(names)
    }


class TestIsHistogramAggregation:
    def test_conditional_aggregation_pinned_to_histogram_type(self) -> None:
        assert is_histogram_aggregation(_column(Function.FUNCTION_SUM).conditional_aggregation)

    def test_histogram_type_nested_in_and_filter(self) -> None:
        aggregation = _column(Function.FUNCTION_SUM, filter=AT_LEAST_250).conditional_aggregation

        assert is_histogram_aggregation(aggregation)

    def test_histogram_type_in_or_filter_is_not_pinned(self) -> None:
        aggregation = AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=VALUE,
            filter=TraceItemFilter(or_filter=OrFilter(filters=[IS_HISTOGRAM, AT_LEAST_250])),
        )

        assert not is_histogram_aggregation(aggregation)

    def test_other_metric_types_are_not_histograms(self) -> None:
        aggregation = AttributeConditionalAggregation(
            aggregate=Function.FUNCTION_SUM,
            key=VALUE,
            filter=TraceItemFilter(
                comparison_filter=ComparisonFilter(
                    key=AttributeKey(type=AttributeKey.TYPE_STRING, name="sentry.metric_type"),
                    op=ComparisonFilter.OP_EQUALS,
                    value=AttributeValue(val_str="histogram_exemplar"),
                )
            ),
        )

        assert not is_histogram_aggregation(aggregation)

    def test_unconditional_aggregation_is_not_a_histogram(self) -> None:
        assert not is_histogram_aggregation(
            AttributeAggregation(aggregate=Function.FUNCTION_SUM, key=VALUE)
        )


@pytest.mark.eap
@pytest.mark.redis_db
class TestHistogramAggregation(BaseApiTest):
    def test_bucket_aggregates_match_design_doc_example(self, setup_histograms: None) -> None:
        functions = [
            Function.FUNCTION_COUNT,
            Function.FUNCTION_SUM,
            Function.FUNCTION_AVG,
            Function.FUNCTION_MIN,
            Function.FUNCTION_MAX,
            Function.FUNCTION_P50,
            Function.FUNCTION_P90,
        ]
        result = _table([_column(function) for function in functions])

        # the exemplar row is skipped; count, sum, avg, min and max are exact
        assert result["latency"] == {
            "FUNCTION_COUNT": 1000,
            "FUNCTION_SUM": pytest.approx(190000),
            "FUNCTION_AVG": pytest.approx(190),
            "FUNCTION_MIN": 102.4,
            "FUNCTION_MAX": 712.9,
            # interpolating inside the buckets gives p50 184.4 and p90 250; the points
            # nearest to those are 187.27 and 246.31, truncated to bfloat16
            "FUNCTION_P50": 187,
            "FUNCTION_P90": 246,
        }
        # rows without buckets, and without histogram_min/max, use their value
        assert result["plain"]["FUNCTION_COUNT"] == 2
        assert result["plain"]["FUNCTION_MIN"] == 10
        assert result["plain"]["FUNCTION_MAX"] == 20
        # sampled rows are weighted by their sample rate
        assert result["sampled"]["FUNCTION_COUNT"] == 6
        assert result["sampled"]["FUNCTION_MAX"] == 10

    def test_percentile_stays_in_its_bucket(self, setup_histograms: None) -> None:
        result = _table([_column(Function.FUNCTION_P90)])

        # interpolating inside (0.4, 5] gives 4.80, the last point of the bucket is 4.885,
        # truncated to bfloat16
        assert result["gap"]["FUNCTION_P90"] == 4.875

    def test_percentiles_across_bucket_layouts_are_ordered(self, setup_histograms: None) -> None:
        functions = [
            Function.FUNCTION_P50,
            Function.FUNCTION_P75,
            Function.FUNCTION_P90,
            Function.FUNCTION_P95,
            Function.FUNCTION_P99,
        ]
        result = _table([_column(function) for function in functions])["mixed"]

        percentiles = [result[Function.Name(function)] for function in functions]
        assert percentiles == sorted(percentiles)
        assert percentiles[0] >= 25
        assert percentiles[-1] <= 100

    def test_value_filter_applies_to_each_bucket_point(self, setup_histograms: None) -> None:
        # filters on the value apply to each point, other filters to the whole row
        result = _table(
            [
                _column(Function.FUNCTION_COUNT, filter=AT_LEAST_250),
                _column(Function.FUNCTION_MIN, filter=AT_LEAST_250),
            ]
        )

        assert result["latency"]["FUNCTION_COUNT"] == 100
        assert result["latency"]["FUNCTION_MIN"] == 256.25

    def test_request_value_filter_is_rejected(self) -> None:
        # it would apply to the mean of each histogram point
        with pytest.raises(BadSnubaRPCRequestException):
            _table([_column(Function.FUNCTION_COUNT)], filter=AT_LEAST_250)
        with pytest.raises(BadSnubaRPCRequestException):
            EndpointTimeSeries().execute(
                TimeSeriesRequest(
                    meta=META,
                    filter=AT_LEAST_250,
                    expressions=[_histogram_count()],
                    granularity_secs=3600,
                )
            )

    def test_mismatched_arrays_fall_back_to_row_value(self, setup_histograms: None) -> None:
        result = _table([_column(Function.FUNCTION_COUNT), _column(Function.FUNCTION_P50)])

        assert result["mismatched"] == {"FUNCTION_COUNT": 1, "FUNCTION_P50": 7}

    def test_aggregations_without_histogram_type_read_rows(self, setup_histograms: None) -> None:
        result = _table(
            [
                _column(Function.FUNCTION_COUNT, histogram=False),
                _column(Function.FUNCTION_MAX, histogram=False),
            ]
        )

        assert result["latency"]["FUNCTION_COUNT"] == 2
        assert result["latency"]["FUNCTION_MAX"] == 1000

    def test_time_series_skips_exemplars_and_confidence(self, setup_histograms: None) -> None:
        response = EndpointTimeSeries().execute(
            TimeSeriesRequest(
                meta=META,
                filter=TraceItemFilter(
                    comparison_filter=ComparisonFilter(
                        key=METRIC_NAME,
                        op=ComparisonFilter.OP_EQUALS,
                        value=AttributeValue(val_str="latency"),
                    )
                ),
                expressions=[_histogram_count()],
                granularity_secs=3600,
            )
        )

        [data_point] = response.result_timeseries[0].data_points
        assert data_point.data == 1000
        # the exemplar row is not a sample
        assert data_point.sample_count == 1
        # the confidence interval math doesn't apply to histogram rows
        assert data_point.reliability == Reliability.RELIABILITY_UNSPECIFIED

    def test_unsupported_function_is_rejected(self) -> None:
        with pytest.raises(BadSnubaRPCRequestException):
            _table([_column(Function.FUNCTION_UNIQ)])
