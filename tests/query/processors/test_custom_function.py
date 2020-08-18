import pytest

from snuba.request.request_settings import HTTPRequestSettings
from snuba.query.processors.custom_function import CustomFunction
from snuba.query.conditions import binary_condition
from snuba.query.logical import Query, SelectedExpression
from snuba.datasets.schemas.tables import TableSource
from snuba.clickhouse.columns import ColumnSet
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
)


TEST_CASES = [
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1")),
            ],
            groupby=[Column("column1", None, "column1")],
            condition=binary_condition(
                None,
                "equals",
                FunctionCall(
                    "group_id", "f", (Column("something", None, "something"),)
                ),
                Literal(None, 1),
            ),
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1")),
            ],
            groupby=[Column("column1", None, "column1")],
            condition=binary_condition(
                None,
                "equals",
                FunctionCall(
                    "group_id", "f", (Column("something", None, "something"),)
                ),
                Literal(None, 1),
            ),
        ),
        id="Function not present",
    ),
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func",
                        "f_call",
                        (Literal(None, "literal1"), Column("param2", None, "param2"),),
                    ),
                ),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func",
                        "f_call_impl",
                        (
                            Literal(None, "literal1"),
                            FunctionCall(
                                None, "inner_call", (Column("param2", None, "param2"),)
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="Expand simple function",
    ),
    pytest.param(
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func",
                        "f_call",
                        (
                            Column("param1", None, "param1"),
                            FunctionCall(
                                None,
                                "assumeNotNull",
                                (Column("param2", None, "param2"),),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func",
                        "f_call_impl",
                        (
                            Column("param1", None, "param1"),
                            FunctionCall(
                                None,
                                "inner_call",
                                (
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (Column("param2", None, "param2"),),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
        ),
        id="Expand simple function",
    ),
]


@pytest.mark.parametrize("query, expected_query", TEST_CASES)
def test_format_expressions(query: Query, expected_query: Query) -> None:
    processor = CustomFunction(
        "f_call", ["param1", "param2"], "f_call_impl(param1, inner_call(param2))",
    )
    # We cannot just run == on the query objects. The content of the two
    # objects is different, being one the AST and the ont the AST + raw body
    processor.process_query(query, HTTPRequestSettings())
    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_arrayjoin_from_ast() == expected_query.get_arrayjoin_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()
