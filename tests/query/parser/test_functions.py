from snuba.query.conditions import binary_condition, BooleanFunctions
from snuba.query.expressions import Column, Literal, FunctionCall
from snuba.query.parser.functions import parse_function_to_expr
from snuba.util import tuplify


def test_complex_conditions_expr() -> None:
    assert parse_function_to_expr(tuplify(["count", []]),) == FunctionCall(
        None, "count", ()
    )
    assert parse_function_to_expr(tuplify(["notEmpty", ["foo"]]),) == FunctionCall(
        None, "notEmpty", (Column(None, "foo", None),)
    )
    assert parse_function_to_expr(
        tuplify(["notEmpty", ["arrayElement", ["foo", 1]]]),
    ) == FunctionCall(
        None,
        "notEmpty",
        (
            FunctionCall(
                None, "arrayElement", (Column(None, "foo", None), Literal(None, 1))
            ),
        ),
    )
    assert parse_function_to_expr(
        tuplify(["foo", ["bar", ["qux"], "baz"]]),
    ) == FunctionCall(
        None,
        "foo",
        (
            FunctionCall(None, "bar", (Column(None, "qux", None),)),
            Column(None, "baz", None),
        ),
    )
    assert parse_function_to_expr(tuplify(["foo", [], "a"]),) == FunctionCall(
        "a", "foo", ()
    )
    assert parse_function_to_expr(tuplify(["foo", ["b", "c"], "d"]),) == FunctionCall(
        "d", "foo", (Column(None, "b", None), Column(None, "c", None))
    )
    assert parse_function_to_expr(tuplify(["foo", ["b", "c", ["d"]]]),) == FunctionCall(
        None,
        "foo",
        (Column(None, "b", None), FunctionCall(None, "c", (Column(None, "d", None),))),
    )

    assert parse_function_to_expr(
        tuplify(["emptyIfNull", ["project_id"]]),
    ) == FunctionCall(None, "emptyIfNull", (Column(None, "project_id", None),))

    assert parse_function_to_expr(
        tuplify(["or", [["or", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.OR,
        binary_condition(
            None, BooleanFunctions.OR, Column(None, "a", None), Column(None, "b", None)
        ),
        Column(None, "c", None),
    )
    assert parse_function_to_expr(
        tuplify(["and", [["and", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.AND,
        binary_condition(
            None, BooleanFunctions.AND, Column(None, "a", None), Column(None, "b", None)
        ),
        Column(None, "c", None),
    )
    # (A OR B) AND C
    assert parse_function_to_expr(
        tuplify(["and", [["or", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.AND,
        binary_condition(
            None, BooleanFunctions.OR, Column(None, "a", None), Column(None, "b", None)
        ),
        Column(None, "c", None),
    )
    # A OR B OR C OR D
    assert parse_function_to_expr(
        tuplify(["or", [["or", [["or", ["c", "d"]], "b"]], "a"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.OR,
        binary_condition(
            None,
            BooleanFunctions.OR,
            binary_condition(
                None,
                BooleanFunctions.OR,
                Column(None, "c", None),
                Column(None, "d", None),
            ),
            Column(None, "b", None),
        ),
        Column(None, "a", None),
    )

    assert parse_function_to_expr(
        tuplify(
            [
                "if",
                [["in", ["release", "tuple", ["'foo'"]]], "release", "'other'"],
                "release",
            ]
        ),
    ) == FunctionCall(
        "release",
        "if",
        (
            FunctionCall(
                None,
                "in",
                (
                    Column(None, "release", None),
                    FunctionCall(None, "tuple", (Literal(None, "foo"),)),
                ),
            ),
            Column(None, "release", None),
            Literal(None, "other"),
        ),
    )

    # TODO once search_message is filled in everywhere, this can be just 'message' again.
    assert parse_function_to_expr(
        tuplify(["positionCaseInsensitive", ["message", "'lol 'single' quotes'"]]),
    ) == FunctionCall(
        None,
        "positionCaseInsensitive",
        (Column(None, "message", None), Literal(None, "lol 'single' quotes")),
    )
