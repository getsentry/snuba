import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import binary_condition
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query
from snuba.query.snql.parser import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH(blah)WHEREa<3COLLECT4-5,3*g(c),c",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    "4-5",
                    FunctionCall(None, "minus", (Literal(None, 4), Literal(None, 5),),),
                ),
                SelectedExpression(
                    "3*g(c)",
                    FunctionCall(
                        None,
                        "multiply",
                        (
                            Literal(None, 3),
                            FunctionCall(None, "g", (Column(None, None, "c"),),),
                        ),
                    ),
                ),
                SelectedExpression("c", Column(None, None, "c"),),
            ],
            condition=binary_condition(
                None, "less", Column(None, None, "a"), Literal(None, 3)
            ),
        ),
        id="Basic query with no spaces and no ambiguous clause content",
    ),
    pytest.param(
        "MATCH (blah) WHERE a<3 COLLECT (2*(4-5)+3), g(c), c BY d, 2+7 ORDER BY f DESC",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    name="(2*(4-5)+3)",
                    expression=FunctionCall(
                        alias=None,
                        function_name="plus",
                        parameters=(
                            FunctionCall(
                                alias=None,
                                function_name="multiply",
                                parameters=(
                                    Literal(alias=None, value=2),
                                    FunctionCall(
                                        alias=None,
                                        function_name="minus",
                                        parameters=(
                                            Literal(alias=None, value=4),
                                            Literal(alias=None, value=5),
                                        ),
                                    ),
                                ),
                            ),
                            Literal(alias=None, value=3),
                        ),
                    ),
                ),
                SelectedExpression(
                    name="g(c)",
                    expression=FunctionCall(
                        alias=None,
                        function_name="g",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="c"),
                        ),
                    ),
                ),
                SelectedExpression(
                    name="c",
                    expression=Column(alias=None, table_name=None, column_name="c"),
                ),
            ],
            condition=binary_condition(
                None, "less", Column(None, None, "a"), Literal(None, 3)
            ),
            groupby=[
                Column(None, None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7),),),
            ],
            order_by=[OrderBy(OrderByDirection.DESC, Column(None, None, "f"))],
        ),
        id="Simple complete query with example of parenthesized arithmetic expression in COLLECT",
    ),
    pytest.param(
        "MATCH (blah) WHERE time_seen<3 AND last_seen=2 AND c=2 AND d=3 COLLECT a",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    name="a",
                    expression=Column(alias=None, table_name=None, column_name="a"),
                )
            ],
            condition=FunctionCall(
                alias=None,
                function_name="and",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="less",
                        parameters=(
                            Column(
                                alias=None, table_name=None, column_name="time_seen"
                            ),
                            Literal(alias=None, value=3),
                        ),
                    ),
                    FunctionCall(
                        alias=None,
                        function_name="and",
                        parameters=(
                            FunctionCall(
                                alias=None,
                                function_name="equals",
                                parameters=(
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="last_seen",
                                    ),
                                    Literal(alias=None, value=2),
                                ),
                            ),
                            FunctionCall(
                                alias=None,
                                function_name="and",
                                parameters=(
                                    FunctionCall(
                                        alias=None,
                                        function_name="equals",
                                        parameters=(
                                            Column(
                                                alias=None,
                                                table_name=None,
                                                column_name="c",
                                            ),
                                            Literal(alias=None, value=2),
                                        ),
                                    ),
                                    FunctionCall(
                                        alias=None,
                                        function_name="equals",
                                        parameters=(
                                            Column(
                                                alias=None,
                                                table_name=None,
                                                column_name="d",
                                            ),
                                            Literal(alias=None, value=3),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple conditions joined by AND",
    ),
    pytest.param(
        "MATCH (blah) WHERE (time_seen<3 OR last_seen=afternoon) OR name=bob COLLECT a",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    name="a",
                    expression=Column(alias=None, table_name=None, column_name="a"),
                )
            ],
            condition=FunctionCall(
                alias=None,
                function_name="or",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="or",
                        parameters=(
                            FunctionCall(
                                alias=None,
                                function_name="less",
                                parameters=(
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="time_seen",
                                    ),
                                    Literal(alias=None, value=3),
                                ),
                            ),
                            FunctionCall(
                                alias=None,
                                function_name="equals",
                                parameters=(
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="last_seen",
                                    ),
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="afternoon",
                                    ),
                                ),
                            ),
                        ),
                    ),
                    FunctionCall(
                        alias=None,
                        function_name="equals",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="name"),
                            Column(alias=None, table_name=None, column_name="bob"),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple conditions joined by OR / parenthesized OR",
    ),
    pytest.param(
        "MATCH (blah) WHERE name!=bob OR last_seen<afternoon AND (location=gps(x,y,z) OR times_seen>0) COLLECT a",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    name="a",
                    expression=Column(alias=None, table_name=None, column_name="a"),
                )
            ],
            condition=FunctionCall(
                alias=None,
                function_name="or",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="notEquals",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="name"),
                            Column(alias=None, table_name=None, column_name="bob"),
                        ),
                    ),
                    FunctionCall(
                        alias=None,
                        function_name="and",
                        parameters=(
                            FunctionCall(
                                alias=None,
                                function_name="less",
                                parameters=(
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="last_seen",
                                    ),
                                    Column(
                                        alias=None,
                                        table_name=None,
                                        column_name="afternoon",
                                    ),
                                ),
                            ),
                            FunctionCall(
                                alias=None,
                                function_name="or",
                                parameters=(
                                    FunctionCall(
                                        alias=None,
                                        function_name="equals",
                                        parameters=(
                                            Column(
                                                alias=None,
                                                table_name=None,
                                                column_name="location",
                                            ),
                                            FunctionCall(
                                                alias=None,
                                                function_name="gps",
                                                parameters=(
                                                    Column(
                                                        alias=None,
                                                        table_name=None,
                                                        column_name="x",
                                                    ),
                                                    Column(
                                                        alias=None,
                                                        table_name=None,
                                                        column_name="y",
                                                    ),
                                                    Column(
                                                        alias=None,
                                                        table_name=None,
                                                        column_name="z",
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                    FunctionCall(
                                        alias=None,
                                        function_name="greater",
                                        parameters=(
                                            Column(
                                                alias=None,
                                                table_name=None,
                                                column_name="times_seen",
                                            ),
                                            Literal(alias=None, value=0),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        id="Query with multiple / complex conditions joined by parenthesized / regular AND / OR",
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_cases)
def test_format_expressions(query_body: str, expected_query: Query) -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")
    query = parse_snql_query(query_body, events)

    assert (
        query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()
    assert query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert query.get_having_from_ast() == expected_query.get_having_from_ast()
