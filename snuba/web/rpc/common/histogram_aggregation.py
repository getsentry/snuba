"""
Aggregations over pre-aggregated histogram rows (HISTOGRAM_MODE_BUCKETS).

Relay stores an OTLP histogram data point as one trace metric row per non-empty
bucket, plus one row per exemplar. Bucket rows hold the bucket bounds, the number of
observations in the bucket and the bucket's share of the data point's sum. Every
other row (SDK distributions, OTLP gauges and counters) is one observation of
`sentry.value`, which we treat as a bucket with zero width and a count of 1.
"""

from collections.abc import Callable

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, Function

from snuba.query.dsl import Functions as f
from snuba.query.dsl import if_cond, literal
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.web.rpc.common.common import get_field_existence_expression
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException

_VALUE_KEY = "sentry.value"
_ROW_KIND_KEY = "sentry._internal.row_kind"
_BUCKET_LOWER_KEY = "sentry._internal.bucket_lower"
_BUCKET_UPPER_KEY = "sentry._internal.bucket_upper"
_BUCKET_COUNT_KEY = "sentry._internal.bucket_count"
_SUM_SHARE_KEY = "sentry._internal.sum_share"

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


def not_exemplar_condition(
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
) -> Expression:
    row_kind = attribute_key_to_expression(
        AttributeKey(name=_ROW_KIND_KEY, type=AttributeKey.TYPE_STRING)
    )
    return f.notEquals(f.ifNull(row_kind, literal("")), literal("exemplar"))


def bucket_aggregation_to_expression(
    aggregation: AttributeConditionalAggregation,
    field: Expression,
    condition: Expression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    sampling_weight: Expression | None,
) -> Expression:
    if (
        aggregation.HasField("expression")
        or aggregation.key.name != _VALUE_KEY
        or aggregation.key.type not in (AttributeKey.TYPE_DOUBLE, AttributeKey.TYPE_FLOAT)
        or aggregation.aggregate not in _SUPPORTED_FUNCTIONS
    ):
        raise BadSnubaRPCRequestException(
            f"HISTOGRAM_MODE_BUCKETS is not supported for {Function.Name(aggregation.aggregate)} "
            f"on {aggregation.key.name or 'an expression'}"
        )

    def bucket_attribute(name: str, default: Expression) -> Expression:
        attribute = attribute_key_to_expression(
            AttributeKey(name=name, type=AttributeKey.TYPE_DOUBLE)
        )
        exists = get_field_existence_expression(attribute)
        return FunctionCall(None, "if", (exists, attribute, default))

    lower = bucket_attribute(_BUCKET_LOWER_KEY, field)
    upper = bucket_attribute(_BUCKET_UPPER_KEY, field)
    count = bucket_attribute(_BUCKET_COUNT_KEY, literal(1.0))
    sum_share = bucket_attribute(_SUM_SHARE_KEY, field)
    if sampling_weight is not None:
        count = f.multiply(count, sampling_weight)
        sum_share = f.multiply(sum_share, sampling_weight)

    alias = aggregation.label or None
    alias_dict = {"alias": alias} if alias else {}
    match aggregation.aggregate:
        case Function.FUNCTION_COUNT:
            return f.round(f.sumIfOrNull(count, condition), **alias_dict)
        case Function.FUNCTION_SUM:
            return f.sumIfOrNull(sum_share, condition, **alias_dict)
        case Function.FUNCTION_AVG | Function.FUNCTION_AVERAGE:
            return f.divide(
                f.sumIfOrNull(sum_share, condition),
                f.sumIfOrNull(count, condition),
                **alias_dict,
            )
        case Function.FUNCTION_MIN:
            return f.minIfOrNull(lower, condition, **alias_dict)
        case Function.FUNCTION_MAX:
            return f.maxIfOrNull(upper, condition, **alias_dict)
    return _bucket_percentile(
        _PERCENTILES[aggregation.aggregate], lower, upper, count, condition, alias
    )


def _bucket_percentile(
    percentile: float,
    lower: Expression,
    upper: Expression,
    count: Expression,
    condition: Expression,
    alias: str | None,
) -> Expression:
    """
    Finds the bucket that holds the requested rank and interpolates linearly inside it.
    sumMap merges equal buckets across rows and sorts them by (lower, upper). The
    intermediate expressions are aliased so the SQL only spells each of them out once.
    """
    histogram = f.sumMapIf(
        f.array(f.tuple(lower, upper)), f.array(count), condition, alias=f"{alias}__histogram"
    )
    buckets = f.tupleElement(histogram, literal(1))
    counts = f.tupleElement(histogram, literal(2))
    cumulative = f.arrayCumSum(counts, alias=f"{alias}__cumulative")
    # the last cumulative count is the total, so some bucket always reaches the rank
    rank = f.multiply(
        literal(percentile), f.arrayElement(cumulative, literal(-1)), alias=f"{alias}__rank"
    )
    index = f.arrayFirstIndex(
        Lambda(None, ("c",), f.greaterOrEquals(Argument(None, "c"), rank)),
        cumulative,
        alias=f"{alias}__index",
    )
    bucket = f.arrayElement(buckets, index, alias=f"{alias}__bucket")
    bucket_lower = f.tupleElement(bucket, literal(1))
    bucket_upper = f.tupleElement(bucket, literal(2))
    # arrayElement returns 0 for index 0, which is the rank below the first bucket
    rank_below = f.arrayElement(cumulative, f.minus(index, literal(1)))
    interpolated = f.plus(
        bucket_lower,
        f.divide(
            f.multiply(f.minus(bucket_upper, bucket_lower), f.minus(rank, rank_below)),
            f.arrayElement(counts, index),
        ),
    )
    return if_cond(f.greater(rank, literal(0)), interpolated, literal(None), alias=alias)
