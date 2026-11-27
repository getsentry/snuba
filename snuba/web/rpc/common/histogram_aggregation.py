"""
Aggregations over pre-aggregated OTLP histogram rows.

Experimental: only used while early adopters validate OTLP histograms end to end, behind
the Sentry and Relay flags (EXP-1484). Revisit or remove this module when that's done.

An aggregation reads them when its filter selects `sentry.metric_type` = `histogram`, a type
only OTLP histograms have, so every other request produces the same SQL as before.

Relay stores an OTLP histogram data point as one trace metric row that holds the bucket
bounds, the bucket counts and each bucket's share of the point's sum as parallel arrays,
plus one `histogram_exemplar` row per exemplar. Each bucket is spread over
POINTS_PER_BUCKET evenly spaced points inside the row and the percentiles run over those
points with -ArrayIf combinators, so the generated SQL stays small. Count and sum add up the
bucket arrays directly. A row without buckets is one point at `sentry.value`.
"""

from collections.abc import Callable, Iterator

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter, TraceItemFilter

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import if_cond, literal
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.web.rpc.common.common import get_field_existence_expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

POINTS_PER_BUCKET = 20
HISTOGRAM_METRIC_TYPE = "histogram"

_METRIC_TYPE = AttributeKey(name="sentry.metric_type", type=AttributeKey.TYPE_STRING)
_VALUE = AttributeKey(name="sentry.value", type=AttributeKey.TYPE_DOUBLE)
_LOWERS = AttributeKey(name="sentry._internal.bucket_lowers", type=AttributeKey.TYPE_ARRAY_DOUBLE)
_UPPERS = AttributeKey(name="sentry._internal.bucket_uppers", type=AttributeKey.TYPE_ARRAY_DOUBLE)
_SUM_SHARES = AttributeKey(
    name="sentry._internal.bucket_sum_shares", type=AttributeKey.TYPE_ARRAY_DOUBLE
)
_COUNTS = AttributeKey(name="sentry._internal.bucket_counts", type=AttributeKey.TYPE_ARRAY_INT)
_MIN = AttributeKey(name="sentry._internal.histogram_min", type=AttributeKey.TYPE_DOUBLE)
_MAX = AttributeKey(name="sentry._internal.histogram_max", type=AttributeKey.TYPE_DOUBLE)

_PERCENTILES = {
    Function.FUNCTION_P50: 0.5,
    Function.FUNCTION_P75: 0.75,
    Function.FUNCTION_P90: 0.9,
    Function.FUNCTION_P95: 0.95,
    Function.FUNCTION_P99: 0.99,
}

_SUPPORTED_FUNCTIONS = {
    Function.FUNCTION_COUNT,
    Function.FUNCTION_SUM,
    Function.FUNCTION_AVG,
    Function.FUNCTION_AVERAGE,
    Function.FUNCTION_MIN,
    Function.FUNCTION_MAX,
    *_PERCENTILES,
}


def _is_histogram_type(trace_filter: TraceItemFilter) -> bool:
    # =, IN, and either side of AND or OR can select histogram rows. Rows without buckets
    # that such a filter also selects are aggregated as one point at `sentry.value`.
    match trace_filter.WhichOneof("value"):
        case "and_filter":
            return any(_is_histogram_type(child) for child in trace_filter.and_filter.filters)
        case "or_filter":
            return any(_is_histogram_type(child) for child in trace_filter.or_filter.filters)
        case "comparison_filter":
            comparison = trace_filter.comparison_filter
            if comparison.key.name != _METRIC_TYPE.name:
                return False
            match comparison.op:
                case ComparisonFilter.OP_EQUALS:
                    return comparison.value.val_str == HISTOGRAM_METRIC_TYPE
                case ComparisonFilter.OP_IN:
                    return HISTOGRAM_METRIC_TYPE in comparison.value.val_str_array.values
    return False


def is_histogram_aggregation(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> bool:
    return (
        isinstance(aggregation, AttributeConditionalAggregation)
        and aggregation.HasField("filter")
        and _is_histogram_type(aggregation.filter)
    )


def _has_buckets(attribute_key_to_expression: Callable[[AttributeKey], Expression]) -> FunctionCall:
    # a row whose arrays don't line up is treated as a row without buckets
    counts_length = f.length(attribute_key_to_expression(_COUNTS))
    return FunctionCall(
        "__histogram_has_buckets",
        "and",
        (
            f.notEmpty(attribute_key_to_expression(_COUNTS)),
            *(
                f.equals(counts_length, f.length(attribute_key_to_expression(key)))
                for key in (_LOWERS, _UPPERS, _SUM_SHARES)
            ),
        ),
    )


def _per_point(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    buckets: FunctionCall,
    plain: Expression,
    alias: str,
) -> FunctionCall:
    return if_cond(
        _has_buckets(attribute_key_to_expression),
        f.arrayFlatten(buckets),
        f.array(plain),
        alias=alias,
    )


def _row_total(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    bucket_values: AttributeKey,
    plain: Expression,
) -> FunctionCall:
    """Adds up a row's bucket values, `plain` for a row without buckets."""
    return if_cond(
        _has_buckets(attribute_key_to_expression),
        f.arraySum(attribute_key_to_expression(bucket_values)),
        plain,
    )


def _integer_weight(weight: Expression) -> FunctionCall:
    # quantileBFloat16Weighted only takes integer weights, scaling by 1000 keeps the
    # fractional ones (a bucket's count split over its points, sampling weights below 1)
    return f.toUInt64(f.round(f.multiply(weight, 1000)))


def _point_weights(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    weight: Expression,
    alias: str,
) -> FunctionCall:
    """Splits each bucket's count evenly over the bucket's points."""
    point_weight = _integer_weight(
        f.divide(f.multiply(Argument(None, "bucket_count"), weight), POINTS_PER_BUCKET)
    )
    return _per_point(
        attribute_key_to_expression,
        f.arrayMap(
            Lambda(None, ("bucket_count",), f.arrayWithConstant(POINTS_PER_BUCKET, point_weight)),
            attribute_key_to_expression(_COUNTS),
        ),
        _integer_weight(weight),
        alias,
    )


def _points(attribute_key_to_expression: Callable[[AttributeKey], Expression]) -> FunctionCall:
    bucket_lower = Argument(None, "bucket_lower")
    bucket_upper = Argument(None, "bucket_upper")
    offset = f.divide(f.plus(Argument(None, "point_index"), 0.5), POINTS_PER_BUCKET)
    bucket_points = f.arrayMap(
        Lambda(
            None,
            ("point_index",),
            f.plus(bucket_lower, f.multiply(f.minus(bucket_upper, bucket_lower), offset)),
        ),
        f.range(POINTS_PER_BUCKET),
    )
    return _per_point(
        attribute_key_to_expression,
        f.arrayMap(
            Lambda(None, ("bucket_lower", "bucket_upper"), bucket_points),
            attribute_key_to_expression(_LOWERS),
            attribute_key_to_expression(_UPPERS),
        ),
        attribute_key_to_expression(_VALUE),
        "__histogram_points",
    )


def _row_bound(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    point_bound: AttributeKey,
    bucket_bounds: AttributeKey,
    array_bound: Callable[[Expression], FunctionCall],
) -> FunctionCall:
    """The point's min or max, else the outermost bucket bound, else `sentry.value`."""
    point = attribute_key_to_expression(point_bound)
    buckets = attribute_key_to_expression(bucket_bounds)
    return f.multiIf(
        get_field_existence_expression(point),
        point,
        f.notEmpty(buckets),
        array_bound(buckets),
        attribute_key_to_expression(_VALUE),
    )


def bucket_aggregation_to_expression(
    aggregation: AttributeConditionalAggregation,
    condition: Expression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    sampling_weight: Expression | None,
) -> Expression:
    if _references_value(aggregation.filter):
        raise BadSnubaRPCRequestException(
            "A filter on sentry.value is not supported for histogram metrics, it would apply "
            "to the mean of each histogram point"
        )
    if (
        aggregation.HasField("expression")
        or aggregation.key.name != _VALUE.name
        or aggregation.key.type not in (AttributeKey.TYPE_DOUBLE, AttributeKey.TYPE_FLOAT)
        or aggregation.aggregate not in _SUPPORTED_FUNCTIONS
    ):
        raise BadSnubaRPCRequestException(
            f"{Function.Name(aggregation.aggregate)} on "
            f"{aggregation.key.name or 'an expression'} is not supported for histogram metrics"
        )

    weight = sampling_weight if sampling_weight is not None else literal(1.0)
    row_count = f.multiply(_row_total(attribute_key_to_expression, _COUNTS, literal(1)), weight)
    row_sum = f.multiply(
        _row_total(attribute_key_to_expression, _SUM_SHARES, attribute_key_to_expression(_VALUE)),
        weight,
    )

    alias_dict = {"alias": aggregation.label} if aggregation.label else {}
    match aggregation.aggregate:
        case Function.FUNCTION_COUNT:
            return f.round(f.sumIfOrNull(row_count, condition), **alias_dict)
        case Function.FUNCTION_SUM:
            return f.sumIfOrNull(row_sum, condition, **alias_dict)
        case Function.FUNCTION_AVG | Function.FUNCTION_AVERAGE:
            return f.divide(
                f.sumIfOrNull(row_sum, condition),
                f.sumIfOrNull(row_count, condition),
                **alias_dict,
            )
        case Function.FUNCTION_MIN:
            row_min = _row_bound(attribute_key_to_expression, _MIN, _LOWERS, f.arrayMin)
            return f.minIfOrNull(row_min, condition, **alias_dict)
        case Function.FUNCTION_MAX:
            row_max = _row_bound(attribute_key_to_expression, _MAX, _UPPERS, f.arrayMax)
            return f.maxIfOrNull(row_max, condition, **alias_dict)
    # BFloat16 keeps percentiles both fast and accurate over the bucket points:
    # - t-digest merges points across bucket boundaries, so it interpolates into empty
    #   buckets and can return values no observation had. Its result also depends on row order.
    # - exact keeps every distinct point, and every row's outer buckets end at that row's own
    #   min and max, so its memory grows with the number of rows.
    # - BFloat16 keys its state by the 16-bit value, so its memory is fixed (at most 65536
    #   keys) however many rows it reads. The cost is precision: it truncates values to 8
    #   bits of mantissa, so a result can be up to 0.78% closer to zero.
    # the weights only depend on the extrapolation mode, so percentiles share them
    mode = ExtrapolationMode.Name(aggregation.extrapolation_mode)
    return cf.quantileBFloat16WeightedArrayIfOrNull(_PERCENTILES[aggregation.aggregate])(
        _points(attribute_key_to_expression),
        _point_weights(attribute_key_to_expression, weight, f"__histogram_weights_{mode}"),
        condition,
        **alias_dict,
    )


def _messages(message: ProtobufMessage) -> Iterator[ProtobufMessage]:
    yield message
    for field, value in message.ListFields():
        if field.message_type is None or field.message_type.GetOptions().map_entry:
            continue
        values = value if field.label == field.LABEL_REPEATED else [value]
        for child in values:
            yield from _messages(child)


def _references_value(trace_filter: TraceItemFilter) -> bool:
    return any(
        isinstance(m, AttributeKey) and m.name == _VALUE.name for m in _messages(trace_filter)
    )


def reject_value_filter_with_buckets(request: TraceItemTableRequest | TimeSeriesRequest) -> None:
    """
    A histogram row's `sentry.value` is only the mean of its point, so a filter on it can't
    be combined with histogram aggregations.
    """
    if not request.HasField("filter") or not _references_value(request.filter):
        return
    if any(
        isinstance(m, (AttributeAggregation, AttributeConditionalAggregation))
        and is_histogram_aggregation(m)
        for m in _messages(request)
    ):
        raise BadSnubaRPCRequestException(
            "A filter on sentry.value can't be combined with histogram metric aggregations, "
            "it would apply to the mean of each histogram point"
        )
