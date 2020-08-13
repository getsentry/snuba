import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.expressions import Argument, Column, Lambda, Literal, FunctionCall
from snuba.query.parser.functions import parse_function_to_expr
from snuba.util import tuplify


test_data = [
    (tuplify(["count", []]), FunctionCall(None, "count", ())),
    (
        tuplify(["notEmpty", ["foo"]]),
        FunctionCall(None, "notEmpty", (Column(None, None, "foo"),)),
    ),
    (
        tuplify(["notEmpty", ["arrayElement", ["foo", 1]]]),
        FunctionCall(
            None,
            "notEmpty",
            (
                FunctionCall(
                    None, "arrayElement", (Column(None, None, "foo"), Literal(None, 1))
                ),
            ),
        ),
    ),
    (
        tuplify(["foo", ["bar", ["qux"], "baz"]]),
        FunctionCall(
            None,
            "foo",
            (
                FunctionCall(None, "bar", (Column(None, None, "qux"),)),
                Column(None, None, "baz"),
            ),
        ),
    ),
    (tuplify(["foo", [], "a"]), FunctionCall("a", "foo", ())),
    (
        tuplify(["foo", ["b", "c"], "d"]),
        FunctionCall("d", "foo", (Column(None, None, "b"), Column(None, None, "c"))),
    ),
    (
        tuplify(["foo", ["b", "c", ["d"]]]),
        FunctionCall(
            None,
            "foo",
            (
                Column(None, None, "b"),
                FunctionCall(None, "c", (Column(None, None, "d"),)),
            ),
        ),
    ),
    (
        tuplify(["emptyIfNull", ["project_id"]]),
        FunctionCall(None, "emptyIfNull", (Column(None, None, "project_id"),)),
    ),
    (
        tuplify(["or", [["or", ["a", "b"]], "c"]]),
        binary_condition(
            None,
            BooleanFunctions.OR,
            binary_condition(
                None,
                BooleanFunctions.OR,
                Column(None, None, "a"),
                Column(None, None, "b"),
            ),
            Column(None, None, "c"),
        ),
    ),
    (
        tuplify(["and", [["and", ["a", "b"]], "c"]]),
        binary_condition(
            None,
            BooleanFunctions.AND,
            binary_condition(
                None,
                BooleanFunctions.AND,
                Column(None, None, "a"),
                Column(None, None, "b"),
            ),
            Column(None, None, "c"),
        ),
    ),
    # (A OR B) AND C
    (
        tuplify(["and", [["or", ["a", "b"]], "c"]]),
        binary_condition(
            None,
            BooleanFunctions.AND,
            binary_condition(
                None,
                BooleanFunctions.OR,
                Column(None, None, "a"),
                Column(None, None, "b"),
            ),
            Column(None, None, "c"),
        ),
    ),
    # A OR B OR C OR D
    (
        tuplify(["or", [["or", [["or", ["c", "d"]], "b"]], "a"]]),
        binary_condition(
            None,
            BooleanFunctions.OR,
            binary_condition(
                None,
                BooleanFunctions.OR,
                binary_condition(
                    None,
                    BooleanFunctions.OR,
                    Column(None, None, "c"),
                    Column(None, None, "d"),
                ),
                Column(None, None, "b"),
            ),
            Column(None, None, "a"),
        ),
    ),
    (
        tuplify(
            [
                "if",
                [["in", ["release", "tuple", ["'foo'"]]], "release", "'other'"],
                "release",
            ]
        ),
        FunctionCall(
            "release",
            "if",
            (
                FunctionCall(
                    None,
                    "in",
                    (
                        Column(None, None, "release"),
                        FunctionCall(None, "tuple", (Literal(None, "foo"),)),
                    ),
                ),
                Column(None, None, "release"),
                Literal(None, "other"),
            ),
        ),
    ),
    # TODO once search_message is filled in everywhere, this can be just 'message' again.
    (
        tuplify(["positionCaseInsensitive", ["message", "'lol 'single' quotes'"]]),
        FunctionCall(
            None,
            "positionCaseInsensitive",
            (Column(None, None, "message"), Literal(None, "lol 'single' quotes"),),
        ),
    ),
    (
        tuplify(
            [
                "or",
                [
                    ["equals", ["exception_stacks.type", "b"]],
                    ["equals", ["exception_stacks.type", "c"]],
                ],
            ]
        ),
        binary_condition(
            None,
            BooleanFunctions.OR,
            FunctionCall(
                alias=None,
                function_name="arrayExists",
                parameters=(
                    Lambda(
                        alias=None,
                        parameters=("x",),
                        transformation=FunctionCall(
                            alias=None,
                            function_name="assumeNotNull",
                            parameters=(
                                binary_condition(
                                    None,
                                    ConditionFunctions.EQ,
                                    Argument(alias=None, name="x"),
                                    Literal(alias=None, value="b"),
                                ),
                            ),
                        ),
                    ),
                    Column(
                        alias=None, table_name=None, column_name="exception_stacks.type"
                    ),
                ),
            ),
            FunctionCall(
                alias=None,
                function_name="arrayExists",
                parameters=(
                    Lambda(
                        alias=None,
                        parameters=("x",),
                        transformation=FunctionCall(
                            alias=None,
                            function_name="assumeNotNull",
                            parameters=(
                                binary_condition(
                                    None,
                                    ConditionFunctions.EQ,
                                    Argument(alias=None, name="x"),
                                    Literal(alias=None, value="c"),
                                ),
                            ),
                        ),
                    ),
                    Column(
                        alias=None, table_name=None, column_name="exception_stacks.type"
                    ),
                ),
            ),
        ),
    ),
]


@pytest.mark.parametrize("actual, expected", test_data)
def test_complex_conditions_expr(actual, expected) -> None:
    dataset = get_dataset("events")
    assert parse_function_to_expr(actual, dataset, "") == expected, actual
