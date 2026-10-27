from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    ExtrapolationMode,
    Function,
)

from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.web.rpc.common.aggregation import aggregation_to_expression
from snuba.web.rpc.common.common import attribute_key_to_expression
from snuba.web.rpc.common.histogram_aggregation import is_histogram_aware_aggregation


def _agg(
    function: Function.ValueType,
    key: str = "sentry.value",
    mode: ExtrapolationMode.ValueType = ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
) -> AttributeConditionalAggregation:
    return AttributeConditionalAggregation(
        aggregate=function,
        key=AttributeKey(name=key, type=AttributeKey.TYPE_DOUBLE),
        label="agg",
        extrapolation_mode=mode,
    )


def _sql(aggregation: AttributeConditionalAggregation) -> str:
    return aggregation_to_expression(
        aggregation, attribute_key_to_expression, use_sampling_factor=True
    ).accept(ClickhouseExpressionFormatter())


def test_only_sentry_value_is_histogram_aware() -> None:
    assert is_histogram_aware_aggregation(_agg(Function.FUNCTION_P95))
    assert is_histogram_aware_aggregation(_agg(Function.FUNCTION_COUNT))
    assert not is_histogram_aware_aggregation(_agg(Function.FUNCTION_P95, key="sentry.duration"))
    assert not is_histogram_aware_aggregation(_agg(Function.FUNCTION_UNIQ))


def test_percentile_interpolates_buckets_and_skips_exemplars() -> None:
    for mode in (
        ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
        ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    ):
        sql = _sql(_agg(Function.FUNCTION_P95, mode=mode))
        assert "sumMapIf" in sql
        assert "quantileTDigest" not in sql
        assert "sentry._internal.bucket_lower" in sql
        assert "sentry._internal.bucket_upper" in sql
        assert "'exemplar'" in sql


def test_count_and_sum_use_bucket_count_and_sum_share() -> None:
    count_sql = _sql(_agg(Function.FUNCTION_COUNT))
    assert "sentry._internal.bucket_count" in count_sql
    assert "'exemplar'" in count_sql

    sum_sql = _sql(_agg(Function.FUNCTION_SUM))
    assert "sentry._internal.sum_share" in sum_sql


def test_other_keys_unchanged() -> None:
    sql = _sql(_agg(Function.FUNCTION_P95, key="sentry.duration"))
    assert "quantileTDigestWeightedIfOrNull" in sql
    assert "sumMapIf" not in sql
