"""
Aggregations over pre-aggregated histogram rows (HISTOGRAM_MODE_BUCKETS).

Relay stores an OTLP histogram data point as one trace metric row that holds the bucket
bounds, the bucket counts and each bucket's share of the point's sum as parallel arrays,
plus one row per exemplar. Each bucket is spread over POINTS_PER_BUCKET evenly spaced
points inside the row and the aggregates run over those points with -ArrayIf
combinators, so the generated SQL stays small. A row without buckets is one point at
`sentry.value`. Exemplar rows are skipped.
"""

from collections.abc import Callable, Iterator

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.endpoint_time_series_pb2 import TimeSeriesRequest
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import TraceItemTableRequest
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
    HistogramMode,
)

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, if_cond, literal
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.web.rpc.common.common import (
    get_field_existence_expression,
    trace_item_filters_to_expression,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

POINTS_PER_BUCKET = 10

_VALUE = AttributeKey(name="sentry.value", type=AttributeKey.TYPE_DOUBLE)
_ROW_KIND = AttributeKey(name="sentry._internal.row_kind", type=AttributeKey.TYPE_STRING)
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


def _lambda(params: tuple[str, ...], body: Expression) -> Lambda:
    return Lambda(None, params, body)


def _arg(name: str) -> Argument:
    return Argument(None, name)


def _point_condition(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression | None:
    """
    The aggregation's filter as a condition on one point `p`, if the filter references
    `sentry.value`. A filter that doesn't reference it applies to the whole row.
    """
    if not isinstance(aggregation, AttributeConditionalAggregation) or not aggregation.HasField(
        "filter"
    ):
        return None
    references_value = False

    def point_key_to_expression(key: AttributeKey) -> Expression:
        nonlocal references_value
        if key.name == _VALUE.name and key.type in (
            AttributeKey.TYPE_DOUBLE,
            AttributeKey.TYPE_FLOAT,
        ):
            references_value = True
            return _arg("p")
        return attribute_key_to_expression(key)

    condition = trace_item_filters_to_expression(
        TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED,
        aggregation.filter,
        point_key_to_expression,
        membership_as_has=True,
    )
    return condition if references_value else None


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


def bucket_condition(
    aggregation: AttributeAggregation | AttributeConditionalAggregation,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
    """Skips exemplar rows and rows where no point passes the aggregation's filter."""
    row_kind = attribute_key_to_expression(_ROW_KIND)
    condition: Expression = f.notEquals(f.ifNull(row_kind, ""), "exemplar")
    point_condition = _point_condition(aggregation, attribute_key_to_expression)
    if point_condition is not None:
        points = _points(attribute_key_to_expression)
        condition = and_cond(condition, f.arrayExists(_lambda(("p",), point_condition), points))
    elif isinstance(aggregation, AttributeConditionalAggregation) and aggregation.HasField(
        "filter"
    ):
        condition = and_cond(
            condition,
            trace_item_filters_to_expression(
                TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED,
                aggregation.filter,
                attribute_key_to_expression,
                membership_as_has=True,
            ),
        )
    return condition


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
    field_exists: Expression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    sampling_weight: Expression | None,
) -> Expression:
    if (
        aggregation.HasField("expression")
        or aggregation.key.name != _VALUE.name
        or aggregation.key.type not in (AttributeKey.TYPE_DOUBLE, AttributeKey.TYPE_FLOAT)
        or aggregation.aggregate not in _SUPPORTED_FUNCTIONS
    ):
        raise BadSnubaRPCRequestException(
            f"HISTOGRAM_MODE_BUCKETS is not supported for {Function.Name(aggregation.aggregate)} "
            f"on {aggregation.key.name or 'an expression'}"
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
    condition = and_cond(field_exists, bucket_condition(aggregation, attribute_key_to_expression))

    point_condition = _point_condition(aggregation, attribute_key_to_expression)
    if point_condition is not None:
        mask = FunctionCall(
            f"{aggregation.label}__mask" if aggregation.label else None,
            "arrayMap",
            (_lambda(("p",), point_condition), points),
        )
        keep = _lambda(("x", "m"), _arg("m"))
        points, weights, sums = (f.arrayFilter(keep, a, mask) for a in (points, weights, sums))

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
        # with a filter on the value, the bounds of the matching points
        case Function.FUNCTION_MIN if point_condition is not None:
            return f.minArrayIfOrNull(points, condition, **alias_dict)
        case Function.FUNCTION_MAX if point_condition is not None:
            return f.maxArrayIfOrNull(points, condition, **alias_dict)
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


def reject_value_filter_with_buckets(request: TraceItemTableRequest | TimeSeriesRequest) -> None:
    """
    The request's filter applies to whole rows, and a histogram row's `sentry.value` is
    only the mean of its point, so a filter on it can't be combined with BUCKETS.
    """
    if not request.HasField("filter") or not any(
        isinstance(m, AttributeKey) and m.name == _VALUE.name for m in _messages(request.filter)
    ):
        return
    if any(
        isinstance(m, (AttributeAggregation, AttributeConditionalAggregation))
        and m.histogram_mode == HistogramMode.HISTOGRAM_MODE_BUCKETS
        for m in _messages(request)
    ):
        raise BadSnubaRPCRequestException(
            "A filter on sentry.value can't be combined with HISTOGRAM_MODE_BUCKETS "
            "aggregations, it would apply to the mean of each histogram point. Put it in "
            "the aggregation's filter instead."
        )
