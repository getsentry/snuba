import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    Lambda,
    Argument,
)
from snuba.clickhouse.query import Query
from snuba.query.processors.type_converters.fixedstring_array_column_processor import (
    FixedStringArrayColumnProcessor,
)
from snuba.request.request_settings import HTTPRequestSettings


tests = [
    pytest.param(
        FunctionCall(
            None,
            "has",
            (
                Column(None, None, "column1"),
                Literal(None, "a7d67cf796774551a95be6543cacd459"),
            ),
        ),
        FunctionCall(
            None,
            "has",
            (
                Column(None, None, "column1"),
                FunctionCall(
                    None,
                    "toFixedString",
                    (
                        Literal(None, "a7d67cf796774551a95be6543cacd459"),
                        Literal(None, 32),
                    ),
                ),
            ),
        ),
        "has(column1, toFixedString('a7d67cf796774551a95be6543cacd459', 32))",
        id="has(column1, toFixedString('a7d67cf796774551a95be6543cacd459', 32))",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests)
def test_uuid_array_column_processor(
    unprocessed: Expression, expected: Expression, formatted_value: str,
) -> None:
    unprocessed_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=unprocessed,
    )
    expected_query = Query(
        Table("transactions", ColumnSet([])),
        selected_columns=[SelectedExpression("column2", Column(None, None, "column2"))],
        condition=expected,
    )

    FixedStringArrayColumnProcessor(set(["column1", "column2"])).process_query(
        unprocessed_query, HTTPRequestSettings()
    )
    assert unprocessed_query.get_selected_columns() == [
        SelectedExpression(
            "column2",
            FunctionCall(
                None,
                "arrayMap",
                (
                    Lambda(
                        None,
                        ("x",),
                        FunctionCall(None, "toString", (Argument(None, "x"),)),
                    ),
                    Column(None, None, "column2"),
                ),
            ),
        )
    ]

    assert expected_query.get_condition() == unprocessed_query.get_condition()
    condition = unprocessed_query.get_condition()
    assert condition is not None
    ret = condition.accept(ClickhouseExpressionFormatter())
    assert ret == formatted_value
