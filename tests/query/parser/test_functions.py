from snuba.query.conditions import binary_condition, BooleanFunctions
from snuba.query.expressions import Column, Literal, FunctionCall
from snuba.query.parser.functions import parse_function_to_expr
from snuba.util import tuplify


def test_complex_conditions_expr() -> None:
    assert parse_function_to_expr(tuplify(["count", []]),) == FunctionCall(
        None, "count", ()
    )
    assert parse_function_to_expr(tuplify(["notEmpty", ["foo"]]),) == FunctionCall(
        None, "notEmpty", (Column("foo", None, "foo"),)
    )
    assert parse_function_to_expr(
        tuplify(["notEmpty", ["arrayElement", ["foo", 1]]]),
    ) == FunctionCall(
        None,
        "notEmpty",
        (
            FunctionCall(
                None, "arrayElement", (Column("foo", None, "foo"), Literal(None, 1))
            ),
        ),
    )
    assert parse_function_to_expr(
        tuplify(["foo", ["bar", ["qux"], "baz"]]),
    ) == FunctionCall(
        None,
        "foo",
        (
            FunctionCall(None, "bar", (Column("qux", None, "qux"),)),
            Column("baz", None, "baz"),
        ),
    )
    assert parse_function_to_expr(tuplify(["foo", [], "a"]),) == FunctionCall(
        "a", "foo", ()
    )
    assert parse_function_to_expr(tuplify(["foo", ["b", "c"], "d"]),) == FunctionCall(
        "d", "foo", (Column("b", None, "b"), Column("c", None, "c"))
    )
    assert parse_function_to_expr(tuplify(["foo", ["b", "c", ["d"]]]),) == FunctionCall(
        None,
        "foo",
        (Column("b", None, "b"), FunctionCall(None, "c", (Column("d", None, "d"),))),
    )

    assert parse_function_to_expr(
        tuplify(["emptyIfNull", ["project_id"]]),
    ) == FunctionCall(None, "emptyIfNull", (Column("project_id", None, "project_id"),))

    assert parse_function_to_expr(
        tuplify(["or", [["or", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.OR,
        binary_condition(
            None, BooleanFunctions.OR, Column("a", None, "a"), Column("b", None, "b")
        ),
        Column("c", None, "c"),
    )
    assert parse_function_to_expr(
        tuplify(["and", [["and", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.AND,
        binary_condition(
            None, BooleanFunctions.AND, Column("a", None, "a"), Column("b", None, "b")
        ),
        Column("c", None, "c"),
    )
    # (A OR B) AND C
    assert parse_function_to_expr(
        tuplify(["and", [["or", ["a", "b"]], "c"]]),
    ) == binary_condition(
        None,
        BooleanFunctions.AND,
        binary_condition(
            None, BooleanFunctions.OR, Column("a", None, "a"), Column("b", None, "b")
        ),
        Column("c", None, "c"),
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
                Column("c", None, "c"),
                Column("d", None, "d"),
            ),
            Column("b", None, "b"),
        ),
        Column("a", None, "a"),
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
                    Column("release", None, "release"),
                    FunctionCall(None, "tuple", (Literal(None, "foo"),)),
                ),
            ),
            Column("release", None, "release"),
            Literal(None, "other"),
        ),
    )

    # TODO once search_message is filled in everywhere, this can be just 'message' again.
    assert parse_function_to_expr(
        tuplify(["positionCaseInsensitive", ["message", "'lol 'single' quotes'"]]),
    ) == FunctionCall(
        None,
        "positionCaseInsensitive",
        (Column("message", None, "message"), Literal(None, "lol 'single' quotes")),
    )
