import struct
from typing import Any, Callable, Iterable, List, Tuple

from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
)
from sentry_protos.snuba.v1alpha.trace_item_attribute_pb2 import AttributeKey

from snuba.query.dsl import CurriedFunctions as cf
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, literal
from snuba.query.expressions import Expression
from snuba.web.rpc.common.common import NORMALIZED_COLUMNS, attribute_key_to_expression
from snuba.web.rpc.exceptions import BadSnubaRPCRequestException


class AggregateFunction:
    def __init__(self, expression: Expression, merge: Callable[[Iterable[Any]], Any]):
        self.expression = expression
        self.merge = merge


AGGREGATE_QUANTILE_FUNCTIONS = {
    AggregateBucketRequest.FUNCTION_P50: 0.5,
    AggregateBucketRequest.FUNCTION_P95: 0.95,
    AggregateBucketRequest.FUNCTION_P99: 0.99,
}


# https://github.com/ClickHouse/ClickHouse/blob/f7ca33868b976b92499178d475a21fd72e9badfa/src/AggregateFunctions/QuantileTDigest.h#L104
def interpolate(x: float, x1: float, y1: float, x2: float, y2: float) -> float:
    k = (x - x1) / (x2 - x1)
    return (1 - k) * y1 + k * y2


# this is a port of quantileTDigestMerge(...) in clickhouse, in this file:
# https://github.com/ClickHouse/ClickHouse/blob/f7ca33868b976b92499178d475a21fd72e9badfa/src/AggregateFunctions/QuantileTDigest.h
def merge_t_digests_states(states: Iterable[str], level: float) -> float:
    centroids: List[Tuple[float, float]] = []
    total_count = 0
    for state in states:
        # each state is hex(quantileTDigestState(...))
        # where the state is a varuint of the count of centroids,
        # and each centroid is a tuple (float32 mean, float32 count)
        state_bytes = bytes.fromhex(state)
        # fast-forward over the centroids.size() varint, we don't need it.
        while len(state_bytes) > 0:
            byt = state_bytes[0]
            state_bytes = state_bytes[1:]
            if not (byt & 0x80):
                break

        for (mean, count) in struct.iter_unpack("<ff", state_bytes):
            centroids.append((mean, count))
            total_count += count

    centroids.sort(key=lambda tup: tup[0])
    prev_mean, prev_count = centroids[0]
    if len(centroids) == 1:
        return prev_mean

    x = level * total_count
    prev_x = 0.0
    total = 0
    for (mean, count) in centroids:
        current_x = total + count * 0.5
        if current_x >= x:
            left = prev_x + (0.5 if prev_count == 1 else 0)
            right = current_x - (0.5 if count == 1 else 0)
            if x <= left:
                return prev_mean
            if x >= right:
                return mean
            return interpolate(x, left, prev_mean, right, mean)

        total += count
        prev_mean = mean
        prev_count = count
        prev_x = current_x
    return total_count


def get_aggregate_func(
    request: AggregateBucketRequest,
) -> AggregateFunction:
    key_expr = attribute_key_to_expression(request.key)
    exists_condition: Expression = literal(True)
    if request.key.name not in NORMALIZED_COLUMNS:
        if request.key.type == AttributeKey.TYPE_STRING:
            exists_condition = f.mapContains(
                column("attr_str"), literal(request.key.name)
            )
        else:
            exists_condition = f.mapContains(
                column("attr_num"), literal(request.key.name)
            )
    sampling_weight_expr = column("sampling_weight_2")
    sign_expr = column("sign")
    sampling_weight_times_sign = f.multiply(sampling_weight_expr, sign_expr)

    if request.aggregate == AggregateBucketRequest.FUNCTION_SUM:
        return AggregateFunction(
            expression=f.sum(
                f.multiply(key_expr, sampling_weight_times_sign), alias="sum"
            ),
            merge=lambda values: sum(values),
        )
    if request.aggregate == AggregateBucketRequest.FUNCTION_COUNT:
        return AggregateFunction(
            expression=f.sumIf(
                sampling_weight_times_sign, exists_condition, alias="count"
            ),
            merge=lambda counts: sum(counts),
        )
    if request.aggregate == AggregateBucketRequest.FUNCTION_AVERAGE:
        return AggregateFunction(
            f.tuple(
                f.sum(f.multiply(key_expr, sampling_weight_times_sign)),
                f.sumIf(sampling_weight_times_sign, exists_condition, alias="count"),
                alias="avg",
            ),
            merge=lambda totals_and_counts: sum(next(iter(totals_and_counts)))
            / sum(next(iter(totals_and_counts))),
        )

    if request.aggregate in AGGREGATE_QUANTILE_FUNCTIONS:
        quantile_number = AGGREGATE_QUANTILE_FUNCTIONS[request.aggregate]
        return AggregateFunction(
            expression=f.hex(
                cf.quantileTDigestWeightedState(
                    0
                )(  # the actual quantile doesn't affect the state
                    key_expr, sampling_weight_expr
                ),
                alias="quantile",
            ),
            merge=lambda states: merge_t_digests_states(states, quantile_number),
        )
    raise BadSnubaRPCRequestException(
        f"Aggregate {request.aggregate} had an unknown or unset type"
    )
