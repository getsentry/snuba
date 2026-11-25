"""
Aggregations over pre-aggregated OTLP histogram rows.

An aggregation reads them when its filter pins `sentry.metric_type` to `histogram`, a type
only OTLP histograms have, so every other request produces the same SQL as before.

Relay stores an OTLP histogram data point as one trace metric row that holds the bucket
bounds, the bucket counts and each bucket's share of the point's sum as parallel arrays,
plus one `histogram_exemplar` row per exemplar. Each bucket is spread over
POINTS_PER_BUCKET evenly spaced points inside the row and the aggregates run over those
points with -ArrayIf combinators, so the generated SQL stays small. A row without buckets
is one point at `sentry.value`.
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

_METRIC_TYPE = "sentry.metric_type"
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


def _pins_histogram_type(trace_filter: TraceItemFilter) -> bool:
    match trace_filter.WhichOneof("value"):
        case "and_filter":
            return any(_pins_histogram_type(child) for child in trace_filter.and_filter.filters)
        case "comparison_filter":
            comparison = trace_filter.comparison_filter
            return (
                comparison.key.name == _METRIC_TYPE
                and comparison.op == ComparisonFilter.OP_EQUALS
                and comparison.value.val_str == HISTOGRAM_METRIC_TYPE
            )
    return False


def is_histogram_aggregation(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
) -> bool:
    return (
        isinstance(aggregation, AttributeConditionalAggregation)
        and aggregation.HasField("filter")
        and _pins_histogram_type(aggregation.filter)
    )


def _lambda(params: tuple[str, ...], body: Expression) -> Lambda:
    return Lambda(None, params, body)


def _arg(name: str) -> Argument:
    return Argument(None, name)


def _per_point(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    buckets: FunctionCall,
    plain: Expression,
    alias: str | None,
) -> FunctionCall:
    # a row whose arrays don't line up is treated as a row without buckets
    counts_length = f.length(attribute_key_to_expression(_COUNTS))
    has_buckets = FunctionCall(
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
    return if_cond(has_buckets, f.arrayFlatten(buckets), f.array(plain), alias=alias)


def _spread(values: Expression, weight: Expression) -> FunctionCall:
    """Splits each bucket's value evenly over the bucket's points."""
    share = f.divide(f.multiply(_arg("v"), weight), POINTS_PER_BUCKET)
    return f.arrayMap(_lambda(("v",), f.arrayWithConstant(POINTS_PER_BUCKET, share)), values)


def _points(attribute_key_to_expression: Callable[[AttributeKey], Expression]) -> FunctionCall:
    lower, upper = _arg("l"), _arg("u")
    offset = f.divide(f.plus(_arg("i"), 0.5), POINTS_PER_BUCKET)
    bucket_points = f.arrayMap(
        _lambda(("i",), f.plus(lower, f.multiply(f.minus(upper, lower), offset))),
        f.range(POINTS_PER_BUCKET),
    )
    return _per_point(
        attribute_key_to_expression,
        f.arrayMap(
            _lambda(("l", "u"), bucket_points),
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
    # the arrays only depend on the extrapolation mode, so aggregations share them
    mode = ExtrapolationMode.Name(aggregation.extrapolation_mode)
    points: Expression = _points(attribute_key_to_expression)
    weights: Expression = _per_point(
        attribute_key_to_expression,
        _spread(attribute_key_to_expression(_COUNTS), weight),
        weight,
        f"__histogram_weights_{mode}",
    )
    sums: Expression = _per_point(
        attribute_key_to_expression,
        _spread(attribute_key_to_expression(_SUM_SHARES), weight),
        f.multiply(attribute_key_to_expression(_VALUE), weight),
        f"__histogram_sums_{mode}",
    )

    alias_dict = {"alias": aggregation.label} if aggregation.label else {}
    match aggregation.aggregate:
        case Function.FUNCTION_COUNT:
            return f.round(f.sumArrayIfOrNull(weights, condition), **alias_dict)
        case Function.FUNCTION_SUM:
            return f.sumArrayIfOrNull(sums, condition, **alias_dict)
        case Function.FUNCTION_AVG | Function.FUNCTION_AVERAGE:
            return f.divide(
                f.sumArrayIfOrNull(sums, condition),
                f.sumArrayIfOrNull(weights, condition),
                **alias_dict,
            )
        case Function.FUNCTION_MIN:
            row_min = _row_bound(attribute_key_to_expression, _MIN, _LOWERS, f.arrayMin)
            return f.minIfOrNull(row_min, condition, **alias_dict)
        case Function.FUNCTION_MAX:
            row_max = _row_bound(attribute_key_to_expression, _MAX, _UPPERS, f.arrayMax)
            return f.maxIfOrNull(row_max, condition, **alias_dict)
    # BFloat16 rather than t-digest, which interpolates across empty buckets and depends on
    # row order, or exact, whose state grows with the distinct points (every row's outer
    # buckets end at that row's min and max). It truncates values to 8 bits of mantissa,
    # so results can be up to 0.78% closer to zero. It takes integer weights, scaled so
    # sampling weights below 1 survive.
    integer_weights = f.arrayMap(
        _lambda(("w",), f.toUInt64(f.round(f.multiply(_arg("w"), 1000)))), weights
    )
    return cf.quantileBFloat16WeightedArrayIfOrNull(_PERCENTILES[aggregation.aggregate])(
        points, integer_weights, condition, **alias_dict
    )


def _messages(message: ProtobufMessage) -> Iterator[ProtobufMessage]:
    yield message
    for field, value in message.ListFields():
        if field.message_type is None or field.message_type.GetOptions().map_entry:
            continue
        for child in value if field.label == field.LABEL_REPEATED else [value]:
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
