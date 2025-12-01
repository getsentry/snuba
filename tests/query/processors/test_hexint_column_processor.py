import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.dsl import Functions as f
from snuba.query.dsl import column, if_cond, literal
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.hexint_column_processor import (
    HexIntColumnProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings

tests = [
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            Literal(None, "a" * 16),
        ),
        "equals(column1, 12297829382473034410)",
        id="equals_operator",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(
                None, "tuple", (Literal(None, "a" * 16), Literal(None, "b" * 16))
            ),
        ),
        "in(column1, (12297829382473034410, 13527612320720337851))",
        id="in_operator",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.IN,
            Column(None, None, "column1"),
            FunctionCall(
                None, "array", (Literal(None, "a" * 16), Literal(None, "b" * 16))
            ),
        ),
        "in(column1, [12297829382473034410, 13527612320720337851])",
        id="array_in_operator",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            FunctionCall(None, "toString", (Literal(None, "a" * 16),)),
        ),
        "equals(lower(leftPad(hex(column1), if(greater(length(hex(column1)), 16), 32, 16), '0')), toString('aaaaaaaaaaaaaaaa'))",
        id="non_optimizable_condition_pattern",
    ),
    pytest.param(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "column1"),
            FunctionCall(None, "toString", (Literal(None, f"00{'a' * 14}"),)),
        ),
        "equals(lower(leftPad(hex(column1), if(greater(length(hex(column1)), 16), 32, 16), '0')), toString('00aaaaaaaaaaaaaa'))",
        id="non_optimizable_condition_pattern_with_leading_zeroes",
    ),
]


@pytest.mark.parametrize("unprocessed, formatted_value", tests)
def test_hexint_column_processor(unprocessed: Expression, formatted_value: str) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column1", Column(None, None, "column1"))],
        condition=unprocessed,
    )
    hex = f.hex(column("column1"))

    HexIntColumnProcessor(set(["column1"])).process_query(
        unprocessed_query, HTTPQuerySettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column1",
            f.lower(
                f.leftPad(
                    hex,
                    if_cond(
                        f.greater(
                            f.length(hex),
                            literal(16),
                        ),
                        literal(32),
                        literal(16),
                    ),
                    literal("0"),
                ),
            ),
        )
    ]

    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value


def test_hexint_processor_skips_empty_literal_optimization() -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column1", Column(None, None, "column1"))],
        condition=binary_condition(
            ConditionFunctions.EQ,
            FunctionCall(
                None,
                "cast",
                (Column(None, None, "column1"), Literal(None, "String")),
            ),
            Literal(None, ""),
        ),
    )

    HexIntColumnProcessor(set(["column1"])).process_query(
        unprocessed_query, HTTPQuerySettings()
    )

    condition = unprocessed_query.get_condition()
    assert isinstance(condition, FunctionCall)
    assert condition.function_name == ConditionFunctions.EQ

    lhs, rhs = condition.parameters
    assert isinstance(rhs, Literal)
    assert rhs.value == ""

    assert isinstance(lhs, FunctionCall)
    assert lhs.function_name == "cast"
    cast_arg, cast_target = lhs.parameters
    assert isinstance(cast_target, Literal)
    assert cast_target.value == "String"

    assert isinstance(cast_arg, FunctionCall)
    assert cast_arg.function_name == "lower"
