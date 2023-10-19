import uuid

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.processors.physical.uuid_array_column_processor import (
    UUIDArrayColumnProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings

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
                    "toUUID",
                    (
                        Literal(
                            None, str(uuid.UUID("a7d67cf7-9677-4551-a95b-e6543cacd459"))
                        ),
                    ),
                ),
            ),
        ),
        "has(column1, toUUID('a7d67cf7-9677-4551-a95b-e6543cacd459'))",
        id="has(column1, toUUID('a7d67cf7-9677-4551-a95b-e6543cacd459'))",
    ),
    pytest.param(
        FunctionCall(
            None,
            "arraySlice",
            (
                Column(None, None, "column1"),
                Literal(None, 0),
                Literal(None, 2),
            ),
        ),
        FunctionCall(
            None,
            "arraySlice",
            (
                FunctionCall(
                    None,
                    "arrayMap",
                    (
                        Lambda(
                            None,
                            ("x",),
                            FunctionCall(
                                None,
                                "replaceAll",
                                (
                                    FunctionCall(
                                        None, "toString", (Argument(None, "x"),)
                                    ),
                                    Literal(None, "-"),
                                    Literal(None, ""),
                                ),
                            ),
                        ),
                        Column(None, None, "column1"),
                    ),
                ),
                Literal(None, 0),
                Literal(None, 2),
            ),
        ),
        "arraySlice(arrayMap(x -> replaceAll(toString(x), '-', ''), column1), 0, 2)",
        id="arraySlice(arrayMap(x -> replaceAll(toString(x), '-', ''), column1), 0, 2)",
    ),
]


@pytest.mark.parametrize("unprocessed, expected, formatted_value", tests)
def test_uuid_array_column_processor(
    unprocessed: Expression,
    expected: Expression,
    formatted_value: str,
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

    UUIDArrayColumnProcessor(set(["column1", "column2"])).process_query(
        unprocessed_query, HTTPQuerySettings()
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
                        FunctionCall(
                            None,
                            "replaceAll",
                            (
                                FunctionCall(None, "toString", (Argument(None, "x"),)),
                                Literal(None, "-"),
                                Literal(None, ""),
                            ),
                        ),
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
