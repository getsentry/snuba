import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.snql import parse_snql_query

test_cases = [
    pytest.param(
        "COLLECT 4-5, 3*g(c), c",
        Query(
            {},
            None,
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        alias=None,
                        function_name="minus",
                        parameters=(
                            Literal(alias=None, value=4),
                            Literal(alias=None, value=5),
                        ),
                    ),
                ),
                SelectedExpression(
                    name=None,
                    expression=FunctionCall(
                        alias=None,
                        function_name="multiply",
                        parameters=(
                            Literal(alias=None, value=3),
                            FunctionCall(
                                alias=None,
                                function_name="g",
                                parameters=(
                                    Column(
                                        alias=None, table_name=None, column_name="c"
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                SelectedExpression(
                    name=None,
                    expression=Column(alias=None, table_name=None, column_name="c"),
                ),
            ],
        ),
        id="Simple COLLECT clause example",
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
