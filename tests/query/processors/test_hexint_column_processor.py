import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.query import SelectedExpression
from snuba.query.expressions import Expression
from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.clickhouse.query import Query
from snuba.query.processors.type_converters.hexint_column_processor import (
    HexIntColumnProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings


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
        "in(column1, tuple(12297829382473034410, 13527612320720337851))",
        id="in_operator",
    ),
]


@pytest.mark.parametrize("unprocessed, formatted_value", tests)
def test_hexint_column_processor(unprocessed: Expression, formatted_value: str) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column1", Column(None, None, "column1"))],
        condition=unprocessed,
    )

    HexIntColumnProcessor(set(["column1"])).process_query(
        unprocessed_query, HTTPRequestSettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column1",
            FunctionCall(
                None,
                "lower",
                (FunctionCall(None, "hex", (Column(None, None, "column1"),),),),
            ),
        )
    ]

    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value
