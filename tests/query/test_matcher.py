from typing import Optional

import pytest

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.matchers import (
    Any,
    AnyExpression,
    AnyOptionalString,
    Column,
    FunctionCall,
    MatchResult,
    OptionalString,
    Or,
    Param,
    Pattern,
    String,
)

test_cases = [
    (
        "Single node match",
        Column(None, String("test_col"), OptionalString("table")),
        ColumnExpr("alias_we_don't_care_of", "test_col", "table"),
        MatchResult(),
    ),  # Simple single node column that matches
    (
        "Single node no match",
        Column(None, String("test_col"), None),
        ColumnExpr(None, "not_a_test_col", None),
        None,
    ),  # Simple single node column that does not match
    (
        "Matches a None table name",
        Column(None, None, Param("table_name", AnyOptionalString())),
        ColumnExpr(None, "not_a_test_col", None),
        MatchResult({"table_name": None}),
    ),  # Matches a None table name
    (
        "Matches a column with all fields",
        Column(
            Param("alias", AnyOptionalString()),
            Param("column_name", Any(str)),
            Param("table_name", AnyOptionalString()),
        ),
        ColumnExpr("alias", "test_col", "table_name"),
        MatchResult(
            {"alias": "alias", "column_name": "test_col", "table_name": "table_name"}
        ),
    ),  # Matches a column with all fields
    (
        "Match anything",
        AnyExpression(),
        ColumnExpr(None, "something_irrelevant", None),
        MatchResult(),
    ),  # Pattern that matches anything
    (
        "Union of two patterns - match",
        Or(
            [
                Param("option1", Column(None, String("col_name"), None)),
                Param("option2", Column(None, String("other_col_name"), None)),
            ]
        ),
        ColumnExpr(None, "other_col_name", None),
        MatchResult({"option2": ColumnExpr(None, "other_col_name", None)}),
    ),  # Two patterns in OR. One matches.
    (
        "Union of two patterns - no match",
        Or(
            [
                Param("option1", Column(None, String("col_name"), None)),
                Param("option2", Column(None, String("other_col_name"), None)),
            ]
        ),
        ColumnExpr(None, "none_of_the_two", None),
        None,
    ),  # Two patterns in OR. None matches.
    (
        "Any string match",
        Column(None, Param("col_name", Any(str)), None),
        ColumnExpr(None, "something_relevant", "not_that_we_care_about_the_table"),
        MatchResult({"col_name": "something_relevant"}),
    ),  # Pattern that matches any string and returns it
    (
        "returns the columns in any function",
        FunctionCall(
            Param("alias", OptionalString(None)),
            String("f_name"),
            (
                Param("p_1", Column(None, Any(str), None)),
                Param("p_2", Column(None, Any(str), None)),
            ),
        ),
        FunctionCallExpr(
            None,
            "f_name",
            (
                ColumnExpr(None, "c_name1", None),
                ColumnExpr("another_irrelevant_alias", "c_name2", None),
            ),
        ),
        MatchResult(
            {
                "alias": None,
                "p_1": ColumnExpr(None, "c_name1", None),
                "p_2": ColumnExpr("another_irrelevant_alias", "c_name2", None),
            }
        ),
    ),  # Match any a function given a function name and returns the columns
    (
        "nested parameters no match",
        FunctionCall(
            None,
            String("f_name"),
            (
                FunctionCall(
                    None, String("f"), (Column(None, String("my_col"), None),)
                ),
                Param(
                    "second_function",
                    FunctionCall(None, Param("second_function_name", Any(str)), None),
                ),
            ),
        ),
        FunctionCallExpr(
            "irrelevant",
            "relevant_and_wrong",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, "my_col", None),)),
                FunctionCallExpr(None, "bla", tuple()),
            ),
        ),
        None,
    ),  # Complex structure with nested parameters. No match
    (
        "complex structure matches",
        FunctionCall(
            None,
            String("f_name"),
            (
                FunctionCall(
                    None, String("f"), (Column(None, String("my_col"), None),)
                ),
                Param(
                    "second_function",
                    FunctionCall(None, Param("second_function_name", Any(str)), None),
                ),
            ),
        ),
        FunctionCallExpr(
            "irrelevant",
            "f_name",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, "my_col", None),)),
                FunctionCallExpr(None, "second_name", tuple()),
            ),
        ),
        MatchResult(
            {
                "second_function_name": "second_name",
                "second_function": FunctionCallExpr(None, "second_name", tuple()),
            },
        ),
    ),  # Complex structure with nested parameters. This matches
]


@pytest.mark.parametrize("name, pattern, expression, expected_result", test_cases)
def test_base_expression(
    name: str,
    pattern: Pattern[Expression],
    expression: Expression,
    expected_result: Optional[MatchResult],
) -> None:
    res = pattern.match(expression)
    assert res == expected_result
