import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import (
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
)
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import OrderBy, OrderByDirection, Query, SelectedExpression
from snuba.query.snql import parse_snql_query


test_cases = [
    pytest.param(
        "MATCH (blah) WHERE a<3 COLLECT 4-5, 3*g(c), c BY d, 2+7 HAVING times_seen>1 OR last_seen=2 ORDER BY f DESC",
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
            groupby=[
                Column(None, None, "d"),
                FunctionCall(None, "plus", (Literal(None, 2), Literal(None, 7),),),
            ],
            having=combine_or_conditions(
                [
                    binary_condition(
                        None,
                        "greater",
                        Column(None, None, "times_seen"),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        None,
                        "equals",
                        Column(None, None, "last_seen"),
                        Literal(None, 2),
                    ),
                ]
            ),
            order_by=[OrderBy(OrderByDirection.DESC, Column(None, None, "f"))],
        ),
        id="Example 1",
    ),
    pytest.param(
        "MATCH (blah) WHERE time_seen<3 COLLECT 4-5, 3*g(c), c HAVING b!=1 OR times_seen>1 AND last_seen=2",
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
                None, "less", Column(None, None, "time_seen"), Literal(None, 3)
            ),
            having=combine_or_conditions(
                [
                    binary_condition(
                        None, "notEquals", Column(None, None, "b"), Literal(None, 1)
                    ),
                    combine_and_conditions(
                        [
                            binary_condition(
                                None,
                                "greater",
                                Column(None, None, "times_seen"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                None,
                                "equals",
                                Column(None, None, "last_seen"),
                                Literal(None, 2),
                            ),
                        ]
                    ),
                ],
            ),
        ),
        id="Example 2",
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
