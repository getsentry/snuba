from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.web.rpc.common.aggregation import (
    ExtrapolationContext,
    PercentileExtrapolationContext,
    aggregation_to_expression,
    get_count_column,
)
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


def test_count_column_skips_exemplar_rows() -> None:
    sql = get_count_column(_agg(Function.FUNCTION_P95), attribute_key_to_expression).accept(
        ClickhouseExpressionFormatter()
    )
    assert "'exemplar'" in sql

    other = get_count_column(
        _agg(Function.FUNCTION_P95, key="sentry.duration"), attribute_key_to_expression
    ).accept(ClickhouseExpressionFormatter())
    assert "'exemplar'" not in other


def test_null_percentile_over_exemplar_only_group_is_not_data_present() -> None:
    context = ExtrapolationContext.from_row(
        "agg",
        {
            "agg": None,
            "__snuba_custom_column__confidence_interval$agg$function_type:p95,granularity:0.005,width:0.1": [
                None
            ],
            "__snuba_custom_column__average_sample_rate$agg": 1.0,
            "__snuba_custom_column__count$agg": 2,
        },
    )
    assert isinstance(context, PercentileExtrapolationContext)
    assert not context.is_data_present
    assert context.reliability == Reliability.RELIABILITY_UNSPECIFIED


def test_k_points_mode_expands_buckets_with_array_join(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter, TraceItemFilter
    from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeValue

    from snuba import settings

    monkeypatch.setattr(settings, "OTLP_HISTOGRAM_POINTS_PER_BUCKET", 10)

    p95 = _sql(_agg(Function.FUNCTION_P95))
    assert "arrayJoin(range(10))" in p95
    assert "quantileTDigestWeightedIfOrNull" in p95
    assert "sumMapIf" not in p95

    # a filter on sentry.value is applied to the expanded point value
    count_if = _agg(Function.FUNCTION_COUNT)
    count_if.filter.CopyFrom(
        TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(name="sentry.value", type=AttributeKey.TYPE_DOUBLE),
                op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                value=AttributeValue(val_double=100.0),
            )
        )
    )
    sql = _sql(count_if)
    # the arrayJoin is spelled out once and referenced by alias afterwards
    assert sql.count("arrayJoin(range(10)) AS _hist_point") == 1
    assert sql.count("_hist_point") >= 2
    assert "greaterOrEquals(if(" in sql

    count_column = get_count_column(count_if, attribute_key_to_expression).accept(
        ClickhouseExpressionFormatter()
    )
    assert "arrayJoin(range(10)) AS _hist_point" in count_column
    assert "equals(_hist_point, 0)" in count_column
