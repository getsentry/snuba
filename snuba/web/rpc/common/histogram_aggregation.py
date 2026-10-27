"""PoC: bucket-aware aggregation for OTLP histogram trace metrics.

Relay (``poc/metrics/otlp-delta-histograms``) stores one OTLP histogram data point
as one EAP metric row per non-empty bucket, plus one row per exemplar:

- ``sentry._internal.row_kind``: ``point`` | ``bucket`` | ``exemplar``
- ``sentry._internal.point_id``: shared by every row of one data point
- bucket rows: ``sentry._internal.bucket_lower`` / ``bucket_upper`` / ``bucket_count``
  and ``sentry._internal.sum_share`` (this bucket's share of the point's sum)

Aggregations on ``sentry.value`` are rewritten so that:

- exemplar rows never contribute,
- a bucket row counts as ``bucket_count`` observations (plain rows count as 1),
- ``sum`` uses ``sum_share`` (plain rows use ``sentry.value``),
- ``min``/``max`` use the bucket bounds,
- percentiles interpolate linearly *inside* the bucket that contains the
  requested rank, instead of collapsing each bucket to one value and feeding it
  to t-digest. Plain rows (SDK distributions, gauges, counters) are treated as a
  zero-width bucket ``[value, value]`` with count 1, so they keep working.

This is not production code: it applies to every ``sentry.value`` aggregation
(no proto flag), and the percentile path keeps one map entry per distinct
``(lower, upper)``, which for SDK distributions means one entry per distinct value.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable

from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, Function

from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, literal
from snuba.query.expressions import Argument, Expression, FunctionCall, Lambda
from snuba.web.rpc.common.common import get_field_existence_expression

VALUE_ATTRIBUTE = "sentry.value"

ROW_KIND = "sentry._internal.row_kind"
BUCKET_LOWER = "sentry._internal.bucket_lower"
BUCKET_UPPER = "sentry._internal.bucket_upper"
BUCKET_COUNT = "sentry._internal.bucket_count"
SUM_SHARE = "sentry._internal.sum_share"

ROW_KIND_EXEMPLAR = "exemplar"

_PERCENTILES = {
    Function.FUNCTION_P50: 0.5,
    Function.FUNCTION_P75: 0.75,
    Function.FUNCTION_P90: 0.9,
    Function.FUNCTION_P95: 0.95,
    Function.FUNCTION_P99: 0.99,
}

SUPPORTED_FUNCTIONS = {
    Function.FUNCTION_COUNT,
    Function.FUNCTION_SUM,
    Function.FUNCTION_AVG,
    Function.FUNCTION_AVERAGE,
    Function.FUNCTION_MIN,
    Function.FUNCTION_MAX,
    *_PERCENTILES.keys(),
}


def is_histogram_aware_aggregation(aggregation: AttributeConditionalAggregation) -> bool:
    return (
        not aggregation.HasField("expression")
        and aggregation.key.name == VALUE_ATTRIBUTE
        and aggregation.key.type in (AttributeKey.TYPE_DOUBLE, AttributeKey.TYPE_FLOAT)
        and aggregation.aggregate in SUPPORTED_FUNCTIONS
    )


def _double(
    name: str, attribute_key_to_expression: Callable[[AttributeKey], Expression]
) -> Expression:
    return attribute_key_to_expression(AttributeKey(name=name, type=AttributeKey.TYPE_DOUBLE))


def _if(
    condition: Expression, then: Expression, otherwise: Expression, alias: str | None = None
) -> FunctionCall:
    return FunctionCall(alias, "if", (condition, then, otherwise))


def _or_default(attribute: Expression, default: Expression) -> Expression:
    return _if(get_field_existence_expression(attribute), attribute, default)


def histogram_aware_aggregation(
    aggregation: AttributeConditionalAggregation,
    field: Expression,
    field_exists: Expression,
    condition_in_aggregation: Expression,
    attribute_key_to_expression: Callable[[AttributeKey], Expression],
    sampling_weight: Expression | None,
) -> Expression:
    """Build the bucket-aware aggregate for ``aggregation`` on ``sentry.value``.

    ``sampling_weight`` is ``1 / sampling_factor`` (or the client/server rate) when
    the request extrapolates, ``None`` otherwise.
    """
    alias = aggregation.label or None
    alias_dict = {"alias": alias} if alias else {}

    row_kind = attribute_key_to_expression(
        AttributeKey(name=ROW_KIND, type=AttributeKey.TYPE_STRING)
    )
    not_exemplar = f.notEquals(f.ifNull(row_kind, literal("")), literal(ROW_KIND_EXEMPLAR))
    condition = and_cond(and_cond(field_exists, condition_in_aggregation), not_exemplar)

    observations = f.toFloat64(
        _or_default(_double(BUCKET_COUNT, attribute_key_to_expression), literal(1.0))
    )
    lower = f.toFloat64(_or_default(_double(BUCKET_LOWER, attribute_key_to_expression), field))
    upper = f.toFloat64(_or_default(_double(BUCKET_UPPER, attribute_key_to_expression), field))
    sum_share = f.toFloat64(_or_default(_double(SUM_SHARE, attribute_key_to_expression), field))

    weight: Expression = sampling_weight if sampling_weight is not None else literal(1.0)
    weighted_observations = f.multiply(observations, weight)

    match aggregation.aggregate:
        case Function.FUNCTION_COUNT:
            return f.round(f.sumIfOrNull(weighted_observations, condition), **alias_dict)
        case Function.FUNCTION_SUM:
            return f.sumIfOrNull(f.multiply(sum_share, weight), condition, **alias_dict)
        case Function.FUNCTION_AVG | Function.FUNCTION_AVERAGE:
            return f.divide(
                f.sumIfOrNull(f.multiply(sum_share, weight), condition),
                f.sumIfOrNull(weighted_observations, condition),
                **alias_dict,
            )
        case Function.FUNCTION_MIN:
            return f.minIfOrNull(lower, condition, **alias_dict)
        case Function.FUNCTION_MAX:
            return f.maxIfOrNull(upper, condition, **alias_dict)

    quantile = _PERCENTILES[aggregation.aggregate]
    return _interpolated_quantile(quantile, lower, upper, weighted_observations, condition, alias)


def _interpolated_quantile(
    quantile: float,
    lower: Expression,
    upper: Expression,
    weighted_observations: Expression,
    condition: Expression,
    alias: str | None,
) -> Expression:
    """Histogram quantile with linear interpolation inside the target bucket.

    sumMap merges rows with identical ``(lower, upper)`` (the same bucket across
    points/series) and returns the buckets sorted by ``(lower, upper)``:

        hist   = sumMapIf([(lower, upper)], [count * weight], cond)
        n      = hist.2
        cum    = arrayCumSum(n)
        total  = arraySum(n)
        target = q * total
        i      = first index with cum[i] >= target
        result = lo[i] + (hi[i] - lo[i]) * (target - (cum[i] - n[i])) / n[i]

    The lambda only references its own arguments (target is passed in as a
    constant array) so no aggregate is captured inside it.
    """
    # The intermediate expressions are aliased so ClickHouse SQL only spells each
    # of them out once (later references print the alias). The alias is derived
    # from the inputs so two different percentiles in one query never collide.
    prefix = (
        "_hist_"
        + hashlib.md5(
            repr((quantile, lower, upper, weighted_observations, condition)).encode()
        ).hexdigest()[:12]
    )

    hist = f.sumMapIf(
        f.array(f.tuple(lower, upper)),
        f.array(weighted_observations),
        condition,
        alias=f"{prefix}_map",
    )
    buckets = f.tupleElement(hist, literal(1), alias=f"{prefix}_buckets")
    counts = f.tupleElement(hist, literal(2), alias=f"{prefix}_counts")
    cumulative = f.arrayCumSum(counts, alias=f"{prefix}_cum")
    total = f.arraySum(counts, alias=f"{prefix}_total")
    target = f.multiply(literal(quantile), total, alias=f"{prefix}_target")

    first_index = f.arrayFirstIndex(
        Lambda(
            None,
            ("c", "t"),
            f.greaterOrEquals(Argument(None, "c"), Argument(None, "t")),
        ),
        cumulative,
        f.arrayWithConstant(f.length(cumulative), target),
    )
    # Guard against floating point rounding leaving target slightly above the
    # last cumulative count (arrayFirstIndex would return 0).
    index = _if(
        f.equals(first_index, literal(0)),
        f.length(cumulative),
        first_index,
        alias=f"{prefix}_idx",
    )

    bucket = f.arrayElement(buckets, index, alias=f"{prefix}_bucket")
    bucket_lower = f.tupleElement(bucket, literal(1))
    bucket_upper = f.tupleElement(bucket, literal(2))
    bucket_count = f.arrayElement(counts, index, alias=f"{prefix}_n")
    rank_in_bucket = f.minus(target, f.minus(f.arrayElement(cumulative, index), bucket_count))

    interpolated = f.plus(
        bucket_lower,
        f.divide(
            f.multiply(f.minus(bucket_upper, bucket_lower), rank_in_bucket),
            bucket_count,
        ),
    )
    return _if(f.greater(total, literal(0)), interpolated, literal(None), alias=alias)
