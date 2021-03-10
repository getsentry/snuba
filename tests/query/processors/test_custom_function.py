import pytest
from snuba.clickhouse.columns import ColumnSet, String, UInt
from snuba.query import SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.custom_function import (
    CustomFunction,
    InvalidCustomFunctionCall,
    partial_function,
    simple_function,
)
from snuba.query.validation.signature import Column as ColType
from snuba.request.request_settings import HTTPRequestSettings

TEST_CASES = [
    pytest.param(
        Query(
            None,
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1")),
            ],
            groupby=[Column("column1", None, "column1")],
            condition=binary_condition(
                "equals",
                FunctionCall(
                    "group_id", "f", (Column("something", None, "something"),)
                ),
                Literal(None, 1),
            ),
        ),
        Query(
            None,
            selected_columns=[
                SelectedExpression("column1", Column("column1", None, "column1")),
            ],
            groupby=[Column("column1", None, "column1")],
            condition=binary_condition(
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
            None,
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
            None,
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
                            Literal(None, 420),
                        ),
                    ),
                ),
            ],
        ),
        id="Expand simple function",
    ),
    pytest.param(
        Query(
            None,
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
            None,
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
                            Literal(None, 420),
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
        ColumnSet([("param1", String()), ("param2", UInt(8)), ("other_col", String())]),
        "f_call",
        [("param1", ColType({String})), ("param2", ColType({UInt}))],
        partial_function(
            "f_call_impl(param1, inner_call(param2), my_const)", [("my_const", 420)],
        ),
    )
    # We cannot just run == on the query objects. The content of the two
    # objects is different, being one the AST and the ont the AST + raw body
    processor.process_query(query, HTTPRequestSettings())
    assert query.get_selected_columns() == expected_query.get_selected_columns()
    assert query.get_groupby() == expected_query.get_groupby()
    assert query.get_condition() == expected_query.get_condition()
    assert query.get_arrayjoin() == expected_query.get_arrayjoin()
    assert query.get_having() == expected_query.get_having()
    assert query.get_orderby() == expected_query.get_orderby()


INVALID_QUERIES = [
    pytest.param(
        Query(
            None,
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func", "f_call", (Column("param2", None, "param2"),),
                    ),
                ),
            ],
        ),
        id="Invalid number of parameters",
    ),
    pytest.param(
        Query(
            None,
            selected_columns=[
                SelectedExpression(
                    "my_func",
                    FunctionCall(
                        "my_func",
                        "f_call",
                        (
                            Column("param2", None, "param2"),
                            Column("param1", None, "param1"),
                        ),
                    ),
                ),
            ],
        ),
        id="Inverting parameter types",
    ),
]


@pytest.mark.parametrize("query", INVALID_QUERIES)
def test_invalid_call(query: Query) -> None:
    processor = CustomFunction(
        ColumnSet([("param1", String()), ("param2", UInt(8)), ("other_col", String())]),
        "f_call",
        [("param1", ColType({String})), ("param2", ColType({UInt}))],
        simple_function("f_call_impl(param1, inner_call(param2))"),
    )
    with pytest.raises(InvalidCustomFunctionCall):
        processor.process_query(query, HTTPRequestSettings())
