from typing import Optional

import pytest

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.matcher import (
    AnyExpression,
    AnyString,
    Column,
    Function,
    MatchResult,
    Or,
    Param,
    Pattern,
    String,
)

test_cases = [
    (
        Column(None, String("test_col"), String("table")),
        ColumnExpr("alias_we_don't_care_of", "test_col", "table"),
        MatchResult(),
    ),  # Simple single node column that matches
    (
        Column(None, String("test_col"), None),
        ColumnExpr(None, "not_a_test_col", None),
        None,
    ),  # Simple single node column that does not match
    (
        AnyExpression(),
        ColumnExpr(None, "something_irrelevant", None),
        MatchResult(),
    ),  # Pattern that matches anything
    (
        Or(
            [
                Param("option1", Column(None, String("col_name"), None)),
                Param("option2", Column(None, String("other_col_name"), None)),
            ]
        ),
        ColumnExpr(None, "other_col_name", None),
        MatchResult(expressions={"option2": ColumnExpr(None, "other_col_name", None)},),
    ),
    (
        Column(None, Param("col_name", AnyString()), None),
        ColumnExpr(None, "something_relevant", "not_that_we_care_about_the_table"),
        MatchResult(strings={"col_name": "something_relevant"}),
    ),  # Pattern that matches any string and returns it
    (
        Function(
            None,
            String("f_name"),
            (
                Param("p_1", Column(None, AnyString(), None)),
                Param("p_2", Column(None, AnyString(), None)),
            ),
        ),
        FunctionCall(
            "irrelevant_alias",
            "f_name",
            (
                ColumnExpr(None, "c_name1", None),
                ColumnExpr("another_irrelevant_alias", "c_name2", None),
            ),
        ),
        MatchResult(
            expressions={
                "p_1": ColumnExpr(None, "c_name1", None),
                "p_2": ColumnExpr("another_irrelevant_alias", "c_name2", None),
            }
        ),
    ),  # Match any a function given a function name and returns the columns
    (
        Function(
            None,
            String("f_name"),
            (
                Function(None, String("f"), (Column(None, String("my_col"), None),)),
                Param(
                    "second_function",
                    Function(None, Param("second_function_name", AnyString()), None),
                ),
            ),
        ),
        FunctionCall(
            "irrelevant",
            "relevant_and_wrong",
            (
                FunctionCall(None, "f", (ColumnExpr(None, "my_col", None),)),
                FunctionCall(None, "bla", tuple()),
            ),
        ),
        None,
    ),  # Complex structure with nested parameters. No match
    (
        Function(
            None,
            String("f_name"),
            (
                Function(None, String("f"), (Column(None, String("my_col"), None),)),
                Param(
                    "second_function",
                    Function(None, Param("second_function_name", AnyString()), None),
                ),
            ),
        ),
        FunctionCall(
            "irrelevant",
            "f_name",
            (
                FunctionCall(None, "f", (ColumnExpr(None, "my_col", None),)),
                FunctionCall(None, "second_name", tuple()),
            ),
        ),
        MatchResult(
            strings={"second_function_name": "second_name"},
            expressions={
                "second_function": FunctionCall(None, "second_name", tuple()),
            },
        ),
    ),  # Complex structure with nested parameters. This matches
]


@pytest.mark.parametrize("pattern, expression, expected_result", test_cases)
def test_base_expression(
    pattern: Pattern[Expression],
    expression: Expression,
    expected_result: Optional[MatchResult],
) -> None:
    res = pattern.match(expression)
    assert res == expected_result
