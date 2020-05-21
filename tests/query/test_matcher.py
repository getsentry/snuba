from typing import Optional

import pytest

from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import (
    Any,
    AnyExpression,
    AnyOptionalString,
    Column,
    FunctionCall,
    Literal,
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
    ),
    (
        "Single node no match",
        Column(None, String("test_col"), None),
        ColumnExpr(None, "not_a_test_col", None),
        None,
    ),
    (
        "Matches a None table name",
        Column(None, None, Param("table_name", AnyOptionalString())),
        ColumnExpr(None, "not_a_test_col", None),
        MatchResult({"table_name": None}),
    ),
    (
        "Matches None as table name",
        Column(None, None, Param("table_name", OptionalString(None))),
        ColumnExpr(None, "not_a_test_col", None),
        MatchResult({"table_name": None}),
    ),
    (
        "Not matching a non None table",
        Column(None, None, Param("table_name", OptionalString(None))),
        ColumnExpr(None, "not_a_test_col", "not None"),
        None,
    ),
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
    ),
    (
        "Match anything",
        AnyExpression(),
        ColumnExpr(None, "something_irrelevant", None),
        MatchResult(),
    ),
    (
        "Match a string through Any(str)",
        Column(Param("p_alias", Any(str)), None, None),
        ColumnExpr("alias", "irrelevant", "irrelevant"),
        MatchResult({"p_alias": "alias"}),
    ),
    (
        "Match a None string through Any",
        Column(Param("p_alias", Any(type(None))), None, None),
        ColumnExpr(None, "irrelevant", "irrelevant"),
        MatchResult({"p_alias": None}),
    ),
    (
        "Do not Match a None string through Any",
        Column(Param("p_alias", Any(type(None))), None, None),
        ColumnExpr("alias", "irrelevant", "irrelevant"),
        None,
    ),
    (
        "Match any expression of Column type",
        Any(ColumnExpr),
        ColumnExpr("irrelevant", "irrelevant", "irrelevant"),
        MatchResult(),
    ),
    (
        "Match any expression of Column type within function",
        FunctionCall(None, None, (Param("p1", Any(ColumnExpr)),),),
        FunctionCallExpr(
            "irrelevant",
            "irrelevant",
            (ColumnExpr("relevant", "relevant", "relevant"),),
        ),
        MatchResult({"p1": ColumnExpr("relevant", "relevant", "relevant")}),
    ),
    (
        "Does not match any Column",
        FunctionCall(None, None, (Param("p1", Any(ColumnExpr)),),),
        FunctionCallExpr("irrelevant", "irrelevant", (LiteralExpr(None, "str"),),),
        None,
    ),
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
    ),
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
    ),
    (
        "Or within a Param",
        Param(
            "one_of_the_two",
            Or(
                [
                    Column(None, String("col_name1"), None),
                    Column(None, String("col_name2"), None),
                ]
            ),
        ),
        ColumnExpr("irrelevant", "col_name2", None),
        MatchResult({"one_of_the_two": ColumnExpr("irrelevant", "col_name2", None)}),
    ),
    (
        "Match String Literal",
        Literal(None, OptionalString("value")),
        LiteralExpr("irrelevant", "value"),
        MatchResult(),
    ),
    (
        "Match any string as Literal",
        Literal(None, Any(str)),
        LiteralExpr("irrelevant", "value"),
        MatchResult(),
    ),
    (
        "Does not match an int as Literal",
        Literal(None, Any(str)),
        LiteralExpr("irrelevant", 123),
        None,
    ),
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
    ),
    (
        "matches a function with optional params",
        FunctionCall(
            None,
            None,
            (
                Param("p_1", Column(None, Any(str), None)),
                Param("p_2", Column(None, Any(str), None)),
            ),
            with_optionals=True,
        ),
        FunctionCallExpr(
            "irrelevant",
            "irrelevant",
            (
                ColumnExpr(None, "c_name1", None),
                ColumnExpr("another_irrelevant_alias", "c_name2", None),
                ColumnExpr("optional_1", "optional_1", None),
                ColumnExpr("optional_2", "optional_2", None),
            ),
        ),
        MatchResult(
            {
                "p_1": ColumnExpr(None, "c_name1", None),
                "p_2": ColumnExpr("another_irrelevant_alias", "c_name2", None),
            }
        ),
    ),
    (
        "dows not match even with optionals",
        FunctionCall(
            None,
            None,
            (
                Param("p_1", Column(None, Any(str), None)),
                Param("p_2", Column(None, Any(str), None)),
            ),
            with_optionals=True,
        ),
        FunctionCallExpr(
            "irrelevant", "irrelevant", (ColumnExpr(None, "c_name1", None),),
        ),
        None,
    ),
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
    ),
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
    ),
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


def test_accessors() -> None:
    func = FunctionCall(
        None,
        String("f_name"),
        (
            FunctionCall(None, String("f"), (Column(None, String("my_col"), None),)),
            Param(
                "second_function",
                FunctionCall(None, Param("second_function_name", Any(str)), None),
            ),
        ),
    )

    result = func.match(
        FunctionCallExpr(
            "irrelevant",
            "f_name",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, "my_col", None),)),
                FunctionCallExpr(None, "second_name", tuple()),
            ),
        )
    )

    assert result is not None
    assert result.expression("second_function") == FunctionCallExpr(
        None, "second_name", tuple()
    )
    assert result.scalar("second_function_name") == "second_name"
