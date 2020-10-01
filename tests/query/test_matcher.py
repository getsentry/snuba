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
    TransformedColumn,
)

test_cases = [
    (
        "Single node match",
        Column(OptionalString("table"), String("test_col")),
        ColumnExpr("alias_we_don't_care_of", "table", "test_col"),
        MatchResult(),
    ),
    (
        "Single node no match",
        Column(None, String("test_col")),
        ColumnExpr(None, None, "not_a_test_col"),
        None,
    ),
    (
        "Matches a None table name",
        Column(Param("table_name", AnyOptionalString()), None),
        ColumnExpr(None, None, "not_a_test_col"),
        MatchResult({"table_name": None}),
    ),
    (
        "Matches None as table name",
        Column(Param("table_name", OptionalString(None)), None),
        ColumnExpr(None, None, "not_a_test_col"),
        MatchResult({"table_name": None}),
    ),
    (
        "Not matching a non None table",
        Column(Param("table_name", OptionalString(None)), None),
        ColumnExpr(None, "not None", "not_a_test_col"),
        None,
    ),
    (
        "Matches a column with all fields",
        Column(
            Param("table_name", AnyOptionalString()), Param("column_name", Any(str)),
        ),
        ColumnExpr("alias", "table_name", "test_col"),
        MatchResult({"column_name": "test_col", "table_name": "table_name"}),
    ),
    (
        "Match anything",
        AnyExpression(),
        ColumnExpr(None, None, "something_irrelevant"),
        MatchResult(),
    ),
    (
        "Match a string through Any(str)",
        Column(Param("p_table_name", Any(str)), None),
        ColumnExpr("irrelevant", "table_name", "irrelevant"),
        MatchResult({"p_table_name": "table_name"}),
    ),
    (
        "Match a None string through Any",
        Column(Param("p_table_name", Any(type(None))), None),
        ColumnExpr("irrelevant", None, "irrelevant"),
        MatchResult({"p_table_name": None}),
    ),
    (
        "Do not Match a None string through Any",
        Column(Param("p_table_name", Any(type(None))), None),
        ColumnExpr("irrelevant", "not_none", "irrelevant"),
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
        FunctionCall(None, (Param("p1", Any(ColumnExpr)),)),
        FunctionCallExpr(
            "irrelevant",
            "irrelevant",
            (ColumnExpr("relevant", "relevant", "relevant"),),
        ),
        MatchResult({"p1": ColumnExpr("relevant", "relevant", "relevant")}),
    ),
    (
        "Wrong number of parameters, does not match",
        FunctionCall(None, (Param("p1", Any(ColumnExpr)),)),
        FunctionCallExpr(
            "irrelevant",
            "irrelevant",
            (
                ColumnExpr("relevant", "relevant", "relevant"),
                ColumnExpr("relevant", "relevant", "relevant"),
            ),
        ),
        None,
    ),
    (
        "Does not match any Column",
        FunctionCall(None, (Param("p1", Any(ColumnExpr)),)),
        FunctionCallExpr("irrelevant", "irrelevant", (LiteralExpr(None, "str"),),),
        None,
    ),
    (
        "Union of two patterns - match",
        Or(
            [
                Param("option1", Column(None, String("col_name"))),
                Param("option2", Column(None, String("other_col_name"))),
            ]
        ),
        ColumnExpr(None, None, "other_col_name"),
        MatchResult({"option2": ColumnExpr(None, None, "other_col_name")}),
    ),
    (
        "Union of two patterns - no match",
        Or(
            [
                Param("option1", Column(None, String("col_name"))),
                Param("option2", Column(None, String("other_col_name"))),
            ]
        ),
        ColumnExpr(None, None, "none_of_the_two"),
        None,
    ),
    (
        "Or within a Param",
        Param(
            "one_of_the_two",
            Or([Column(None, String("col_name1")), Column(None, String("col_name2"))]),
        ),
        ColumnExpr("irrelevant", None, "col_name2"),
        MatchResult({"one_of_the_two": ColumnExpr("irrelevant", None, "col_name2")}),
    ),
    (
        "Match String Literal",
        Literal(OptionalString("value")),
        LiteralExpr("irrelevant", "value"),
        MatchResult(),
    ),
    (
        "Match any string as Literal",
        Literal(Any(str)),
        LiteralExpr("irrelevant", "value"),
        MatchResult(),
    ),
    (
        "Does not match an int as Literal",
        Literal(Any(str)),
        LiteralExpr("irrelevant", 123),
        None,
    ),
    (
        "returns the columns in any function",
        FunctionCall(
            String("f_name"),
            (
                Param("p_1", Column(None, Any(str))),
                Param("p_2", Column(None, Any(str))),
            ),
        ),
        FunctionCallExpr(
            None,
            "f_name",
            (
                ColumnExpr(None, None, "c_name1"),
                ColumnExpr("another_irrelevant_alias", None, "c_name2"),
            ),
        ),
        MatchResult(
            {
                "p_1": ColumnExpr(None, None, "c_name1"),
                "p_2": ColumnExpr("another_irrelevant_alias", None, "c_name2"),
            }
        ),
    ),
    (
        "matches a function with optional params",
        FunctionCall(
            None,
            (
                Param("p_1", Column(None, Any(str))),
                Param("p_2", Column(None, Any(str))),
            ),
            with_optionals=True,
        ),
        FunctionCallExpr(
            "irrelevant",
            "irrelevant",
            (
                ColumnExpr(None, None, "c_name1"),
                ColumnExpr("another_irrelevant_alias", None, "c_name2"),
                ColumnExpr("optional_1", None, "optional_1"),
                ColumnExpr("optional_2", None, "optional_2"),
            ),
        ),
        MatchResult(
            {
                "p_1": ColumnExpr(None, None, "c_name1"),
                "p_2": ColumnExpr("another_irrelevant_alias", None, "c_name2"),
            }
        ),
    ),
    (
        "dows not match even with optionals",
        FunctionCall(
            None,
            (
                Param("p_1", Column(None, Any(str))),
                Param("p_2", Column(None, Any(str))),
            ),
            with_optionals=True,
        ),
        FunctionCallExpr(
            "irrelevant", "irrelevant", (ColumnExpr(None, None, "c_name1"),),
        ),
        None,
    ),
    (
        "nested parameters no match",
        FunctionCall(
            String("f_name"),
            (
                FunctionCall(String("f"), (Column(None, String("my_col")),)),
                Param(
                    "second_function",
                    FunctionCall(Param("second_function_name", Any(str)), None),
                ),
            ),
        ),
        FunctionCallExpr(
            "irrelevant",
            "relevant_and_wrong",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, None, "my_col"),)),
                FunctionCallExpr(None, "bla", tuple()),
            ),
        ),
        None,
    ),
    (
        "complex structure matches",
        FunctionCall(
            String("f_name"),
            (
                FunctionCall(String("f"), (Column(None, String("my_col")),)),
                Param(
                    "second_function",
                    FunctionCall(Param("second_function_name", Any(str)), None),
                ),
            ),
        ),
        FunctionCallExpr(
            "irrelevant",
            "f_name",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, None, "my_col"),)),
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
    (
        "transformed column matches bare column",
        TransformedColumn(Column(None, String("my_col"))),
        ColumnExpr("irrelevant", "my_table", "my_col"),
        MatchResult(),
    ),
    (
        "transformed column matches wrapped column",
        TransformedColumn(Column(None, String("my_col"))),
        FunctionCallExpr(
            "irrelevant", "some_fn", (ColumnExpr("irrelevant", "my_table", "my_col"),)
        ),
        MatchResult(),
    ),
    (
        "transformed column matches multi-wrapped column",
        TransformedColumn(Column(None, Param("the_column", String("my_col")))),
        FunctionCallExpr(
            "most_irrelevant",
            "most_fn",
            (
                LiteralExpr(None, 2),
                FunctionCallExpr(
                    "irrelevant",
                    "some_fn",
                    (ColumnExpr("irrelevant", "my_table", "my_col"),),
                ),
            ),
        ),
        MatchResult({"the_column": "my_col"}),
    ),
    (
        "transformed column does not match wrapped other column",
        TransformedColumn(Column(None, String("my_col"))),
        FunctionCallExpr(
            "most_irrelevant",
            "most_fn",
            (
                LiteralExpr(None, 2),
                FunctionCallExpr(
                    "irrelevant",
                    "some_fn",
                    (ColumnExpr("irrelevant", "my_table", "not_my_col"),),
                ),
            ),
        ),
        None,
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
        String("f_name"),
        (
            FunctionCall(String("f"), (Column(None, String("my_col")),)),
            Param(
                "second_function",
                FunctionCall(Param("second_function_name", Any(str)), None),
            ),
        ),
    )

    result = func.match(
        FunctionCallExpr(
            "irrelevant",
            "f_name",
            (
                FunctionCallExpr(None, "f", (ColumnExpr(None, None, "my_col"),)),
                FunctionCallExpr(None, "second_name", tuple()),
            ),
        )
    )

    assert result is not None
    assert result.expression("second_function") == FunctionCallExpr(
        None, "second_name", tuple()
    )
    assert result.scalar("second_function_name") == "second_name"
