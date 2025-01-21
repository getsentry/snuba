import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Table
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
        "equals(lower(hex(column1)), toString('aaaaaaaaaaaaaaaa'))",
        id="non_optimizable_condition_pattern",
    ),
]


@pytest.mark.parametrize("unprocessed, formatted_value", tests)
def test_hexint_column_processor(unprocessed: Expression, formatted_value: str) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[SelectedExpression("column1", Column(None, None, "column1"))],
        condition=unprocessed,
    )

    HexIntColumnProcessor(set(["column1"])).process_query(
        unprocessed_query, HTTPQuerySettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column1",
            FunctionCall(
                None,
                "lower",
                (
                    FunctionCall(
                        None,
                        "hex",
                        (Column(None, None, "column1"),),
                    ),
                ),
            ),
        )
    ]

    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value
