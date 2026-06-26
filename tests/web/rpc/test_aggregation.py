from typing import Any

import pytest
from sentry_protos.snuba.v1.attribute_conditional_aggregation_pb2 import (
    AttributeConditionalAggregation,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    AttributeValue,
    ExtrapolationMode,
    Function,
    IntArray,
    Reliability,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import ComparisonFilter, TraceItemFilter

from snuba.query.expressions import Column, FunctionCall, JsonPath, Literal, SubscriptableReference
from snuba.web.rpc.common.common import (
    attribute_key_to_expression,
    get_field_existence_expression,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    CUSTOM_COLUMN_PREFIX,
    CustomColumnInformation,
    ExtrapolationContext,
    _get_closest_percentile_index,
    aggregation_to_expression,
    get_confidence_interval_column,
)


def test_generate_custom_column_alias() -> None:
    custom_column_information_with_metadata = CustomColumnInformation(
        custom_column_id="confidence_interval",
        referenced_column="count",
        metadata={"function_type": "count", "additional_metadata": "value"},
    )

    assert custom_column_information_with_metadata.to_alias() == (
        CUSTOM_COLUMN_PREFIX
        + "confidence_interval$count$function_type:count,additional_metadata:value"
    )


def test_generate_custom_column_alias_without_metadata() -> None:
    custom_column_information_without_metadata = CustomColumnInformation(
        custom_column_id="column_id",
        referenced_column="count",
        metadata={},
    )

    assert (
        custom_column_information_without_metadata.to_alias()
        == CUSTOM_COLUMN_PREFIX + "column_id$count"
    )


def test_generate_custom_column_alias_without_referenced_column() -> None:
    custom_column_information_without_referenced_column = CustomColumnInformation(
        custom_column_id="column_id",
        referenced_column=None,
        metadata={},
    )

    assert (
        custom_column_information_without_referenced_column.to_alias()
        == CUSTOM_COLUMN_PREFIX + "column_id"
    )


def test_get_custom_column_information() -> None:
    alias = CUSTOM_COLUMN_PREFIX + "column_type$count$meta1:value1,meta2:value2"
    custom_column_information = CustomColumnInformation.from_alias(alias)
    assert custom_column_information == CustomColumnInformation(
        custom_column_id="column_type",
        referenced_column="count",
        metadata={"meta1": "value1", "meta2": "value2"},
    )


def test_get_confidence_interval_column_for_non_extrapolatable_column() -> None:
    assert (
        get_confidence_interval_column(
            AttributeConditionalAggregation(
                aggregate=Function.FUNCTION_MIN,
                key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test"),
                label="min(test)",
                extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
            ),
            attribute_key_to_expression,
        )
        is None
    )


@pytest.mark.parametrize(
    (
        "row_data",
        "column_name",
        "average_sample_rate",
        "reliability",
        "is_data_present",
        "is_extrapolated",
    ),
    [
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "p95(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 20,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0.5,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 120,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "count(sentry.duration)",
            0.5,
            Reliability.RELIABILITY_HIGH,
            True,
            True,
        ),
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "min(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 20,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0.5,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 120,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "min(sentry.duration)",
            0,
            Reliability.RELIABILITY_UNSPECIFIED,
            False,
            False,
        ),
        (
            {
                "time": "2024-4-20 16:20:00",
                "count(sentry.duration)": 100,
                "p95(sentry.duration)": 123456,
                CustomColumnInformation(
                    custom_column_id="confidence_interval",
                    referenced_column="count(sentry.duration)",
                    metadata={"function_type": "count"},
                ).to_alias(): 0,
                CustomColumnInformation(
                    custom_column_id="average_sample_rate",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0,
                CustomColumnInformation(
                    custom_column_id="count",
                    referenced_column="count(sentry.duration)",
                    metadata={},
                ).to_alias(): 0,
                "group_by_attr_1": "g1",
                "group_by_attr_2": "a1",
            },
            "count(sentry.duration)",
            0,
            Reliability.RELIABILITY_UNSPECIFIED,
            False,
            True,
        ),
    ],
)
def test_get_extrapolation_context(
    row_data: dict[str, Any],
    column_name: str,
    average_sample_rate: float,
    reliability: Reliability.ValueType,
    is_data_present: bool,
    is_extrapolated: bool,
) -> None:
    extrapolation_context = ExtrapolationContext.from_row(column_name, row_data)
    assert extrapolation_context.average_sample_rate == average_sample_rate
    assert extrapolation_context.reliability == reliability
    assert extrapolation_context.is_data_present == is_data_present
    assert extrapolation_context.is_extrapolated == is_extrapolated


@pytest.mark.parametrize(
    ("value", "percentile", "granularity", "width", "expected_index"),
    [
        (
            0,
            0.5,
            0.05,
            0.1,
            0,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0 is 0.4
        (
            0.43,
            0.5,
            0.05,
            0.1,
            1,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.43 is 0.45
        (
            0.52,
            0.5,
            0.05,
            0.1,
            2,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.52 is 0.5
        (
            0.8,
            0.5,
            0.05,
            0.1,
            3,
        ),  # possible percentiles are [0.4, 0.45, 0.5, 0.55], closest to 0.8 is 0.55
    ],
)
def test_get_closest_percentile_index(
    value: float,
    percentile: float,
    granularity: float,
    width: float,
    expected_index: int,
) -> None:
    assert _get_closest_percentile_index(value, percentile, granularity, width) == expected_index


def test_attribute_key_to_expression_type_array() -> None:
    from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter

    attr_key = AttributeKey(type=AttributeKey.TYPE_ARRAY, name="user_ids")
    expr = attribute_key_to_expression(attr_key)
    assert isinstance(expr, FunctionCall)
    assert expr.function_name == "toJSONString"
    assert expr.alias == "user_ids_TYPE_ARRAY"
    fmt = ClickhouseExpressionFormatter()
    sql = expr.accept(fmt)
    assert sql == "(toJSONString(attributes_array.`user_ids`::Array(JSON)) AS user_ids_TYPE_ARRAY)"


def test_get_field_existence_expression_array_map() -> None:
    """arrayMap expressions (used for TYPE_ARRAY) should use notEmpty for existence checks."""
    field = FunctionCall(alias="test", function_name="arrayMap", parameters=())
    expr = get_field_existence_expression(field)
    assert isinstance(expr, FunctionCall)
    assert expr.function_name == "notEmpty"


def test_get_field_existence_expression_coalesce() -> None:
    """Coalesced attributes should check existence for all underlying keys with OR."""
    attr_col = Column(alias=None, table_name=None, column_name="attributes_string")
    canonical_ref = SubscriptableReference(
        column=attr_col,
        key=Literal(alias=None, value="http.response.body.size"),
        alias=None,
    )
    deprecated_ref = SubscriptableReference(
        column=attr_col,
        key=Literal(alias=None, value="http.response_content_length"),
        alias=None,
    )
    coalesce_field = FunctionCall(
        alias="test",
        function_name="coalesce",
        parameters=(canonical_ref, deprecated_ref),
    )
    expr = get_field_existence_expression(coalesce_field)
    assert isinstance(expr, FunctionCall)
    assert expr.function_name == "or"
    assert len(expr.parameters) == 2
    lhs, rhs = expr.parameters
    assert isinstance(lhs, FunctionCall) and lhs.function_name == "has"
    assert isinstance(rhs, FunctionCall) and rhs.function_name == "has"
    assert lhs.parameters[1] == Literal(alias=None, value="http.response.body.size")
    assert rhs.parameters[1] == Literal(alias=None, value="http.response_content_length")


def test_aggregation_to_expression_uniq_type_array() -> None:
    agg = AttributeConditionalAggregation(
        aggregate=Function.FUNCTION_UNIQ,
        key=AttributeKey(type=AttributeKey.TYPE_ARRAY, name="user_ids"),
        label="uniq_users",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )
    expr = aggregation_to_expression(agg, attribute_key_to_expression)
    assert isinstance(expr, FunctionCall)
    assert expr.function_name == "round"
    assert expr.alias == "uniq_users"
    inner = expr.parameters[0]
    assert isinstance(inner, FunctionCall)
    assert inner.function_name == "uniqArrayIfOrNull"
    # Must be the stored Array(JSON) path, not toJSONString (String) from attribute_key_to_expression
    first = inner.parameters[0]
    assert isinstance(first, JsonPath)
    assert first.path == "user_ids"
    assert first.return_type == "Array(JSON)"


def test_aggregation_to_expression_sum_type_array_raises() -> None:
    agg = AttributeConditionalAggregation(
        aggregate=Function.FUNCTION_SUM,
        key=AttributeKey(type=AttributeKey.TYPE_ARRAY, name="user_ids"),
        label="sum_users",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
    )
    with pytest.raises(BadSnubaRPCRequestException, match="not supported for array attribute"):
        aggregation_to_expression(agg, attribute_key_to_expression)


def test_conditional_aggregation_uses_has_for_in_sets() -> None:
    """Regression guard for SNUBA-9W6 / SNUBA-A1W (mixed-version distributed reads).

    A conditional aggregation's filter is embedded in a SELECT-clause ``countIf``/
    ``sumIf``. A constant ``IN`` set there bakes a server-generated
    ``__set_<Type>_<hash>_<hash>`` identifier into the result-block column name; on a
    mixed-version cluster the two sides hash it differently and the distributed read
    fails with ``Code: 10 ... Not found column ... While executing Remote.``. The
    membership must therefore be built as ``has(array(...), x)`` instead.
    """
    project_ids = [11, 22, 33]
    agg = AttributeConditionalAggregation(
        aggregate=Function.FUNCTION_SUM,
        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="my.field"),
        label="sum(my.field)",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
        filter=TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(name="sentry.project_id", type=AttributeKey.TYPE_INT),
                op=ComparisonFilter.OP_IN,
                value=AttributeValue(val_int_array=IntArray(values=project_ids)),
            )
        ),
    )

    expr = aggregation_to_expression(agg, attribute_key_to_expression)

    # No in() over a constant array may survive (it would reintroduce the __set_* id).
    in_over_arrays = [
        e
        for e in expr
        if isinstance(e, FunctionCall)
        and e.function_name == "in"
        and len(e.parameters) == 2
        and isinstance(e.parameters[1], FunctionCall)
        and e.parameters[1].function_name == "array"
    ]
    assert not in_over_arrays, (
        "conditional aggregation must not embed an in() over a constant array"
    )

    # ...and the membership is emitted as has(array(<project_ids>), x).
    has_over_pids = [
        e
        for e in expr
        if isinstance(e, FunctionCall)
        and e.function_name == "has"
        and isinstance(e.parameters[0], FunctionCall)
        and e.parameters[0].function_name == "array"
        and [p.value for p in e.parameters[0].parameters if isinstance(p, Literal)] == project_ids
    ]
    assert has_over_pids, "expected has(array(<project_ids>), x) in the conditional aggregate"


def _aggregation_column_names(expr: Any) -> set[str]:
    return {e.column_name for e in expr if isinstance(e, Column)}


def test_conditional_aggregation_array_filter_uses_typed_columns() -> None:
    """A conditional aggregation whose filter is on an array attribute reads the
    typed ``attributes_array_*`` columns when ``use_array_map_columns`` is set, and
    the legacy ``attributes_array`` JSON column otherwise."""
    agg = AttributeConditionalAggregation(
        aggregate=Function.FUNCTION_SUM,
        key=AttributeKey(type=AttributeKey.TYPE_DOUBLE, name="my.field"),
        label="sum(my.field)",
        extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_NONE,
        filter=TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.TYPE_ARRAY, name="my_tags"),
                op=ComparisonFilter.OP_LIKE,
                value=AttributeValue(val_str="%error%"),
            )
        ),
    )

    default_cols = _aggregation_column_names(
        aggregation_to_expression(agg, attribute_key_to_expression)
    )
    assert "attributes_array" in default_cols
    assert not any(c.startswith("attributes_array_") for c in default_cols)

    typed_cols = _aggregation_column_names(
        aggregation_to_expression(agg, attribute_key_to_expression, use_array_map_columns=True)
    )
    # A LIKE pattern can only match string elements, so the array filter reads just the
    # typed string array column — not the other typed columns nor the legacy JSON column.
    assert "attributes_array_string" in typed_cols
    assert "attributes_array_int" not in typed_cols
    assert "attributes_array_float" not in typed_cols
    assert "attributes_array_bool" not in typed_cols
    assert "attributes_array" not in typed_cols
