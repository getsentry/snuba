import pytest

from typing import Any, Sequence

from snuba.datasets.factory import get_dataset
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    Argument,
)
from snuba.query.conditions import (
    ConditionFunctions,
    BooleanFunctions,
)
from snuba.query.parser.conditions import parse_conditions_to_expr
from snuba.util import tuplify

test_conditions = [
    ([], None,),
    ([[[]], []], None,),
    (
        [["a", "=", 1]],
        FunctionCall(
            None, ConditionFunctions.EQ, (Column(None, None, "a"), Literal(None, 1))
        ),
    ),
    (
        [["a", "=", "'nice \n a newline\n'"]],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (Column(None, None, "a"), Literal(None, "'nice \n a newline\n'")),
        ),
    ),
    (
        [[["a", "=", 1]]],
        FunctionCall(
            None, ConditionFunctions.EQ, (Column(None, None, "a"), Literal(None, 1))
        ),
    ),
    (
        [["a", "=", 1], ["b", "=", 2]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "a"), Literal(None, 1)),
                ),
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "b"), Literal(None, 2)),
                ),
            ),
        ),
    ),
    (
        [["a", "=", 1], ["b", "=", 2], ["c", "=", 3]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "a"), Literal(None, 1)),
                ),
                FunctionCall(
                    None,
                    BooleanFunctions.AND,
                    (
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "b"), Literal(None, 2)),
                        ),
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "c"), Literal(None, 3)),
                        ),
                    ),
                ),
            ),
        ),
    ),  # Odd number of conditions. Right associative expression
    (
        [[["a", "=", 1], ["b", "=", 2]]],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "a"), Literal(None, 1)),
                ),
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "b"), Literal(None, 2)),
                ),
            ),
        ),
    ),
    (
        [[["a", "=", 1], ["b", "=", 2], ["c", "=", 3]]],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "a"), Literal(None, 1)),
                ),
                FunctionCall(
                    None,
                    BooleanFunctions.OR,
                    (
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "b"), Literal(None, 2)),
                        ),
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "c"), Literal(None, 3)),
                        ),
                    ),
                ),
            ),
        ),
    ),  # Odd number of conditions. Right associative expression
    (
        [[["a", "=", 1], ["b", "=", 2]], ["c", "=", 3]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    BooleanFunctions.OR,
                    (
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "a"), Literal(None, 1)),
                        ),
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "b"), Literal(None, 2)),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "c"), Literal(None, 3)),
                ),
            ),
        ),
    ),
    (
        [[["a", "=", 1], ["b", "=", 2]], [["c", "=", 3], ["d", "=", 4]]],
        FunctionCall(
            None,
            BooleanFunctions.AND,
            (
                FunctionCall(
                    None,
                    BooleanFunctions.OR,
                    (
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "a"), Literal(None, 1)),
                        ),
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "b"), Literal(None, 2)),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    BooleanFunctions.OR,
                    (
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "c"), Literal(None, 3)),
                        ),
                        FunctionCall(
                            None,
                            ConditionFunctions.EQ,
                            (Column(None, None, "d"), Literal(None, 4)),
                        ),
                    ),
                ),
            ),
        ),
    ),
    (
        [[["a", "=", 1], []]],
        FunctionCall(
            None, ConditionFunctions.EQ, (Column(None, None, "a"), Literal(None, 1)),
        ),
    ),  # Malformed Condition Input
    (
        [[[["tag", ["foo"]], "=", 1], ["b", "=", 2]]],
        FunctionCall(
            None,
            BooleanFunctions.OR,
            (
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (
                        FunctionCall(None, "tag", (Column(None, None, "foo"),)),
                        Literal(None, 1),
                    ),
                ),
                FunctionCall(
                    None,
                    ConditionFunctions.EQ,
                    (Column(None, None, "b"), Literal(None, 2)),
                ),
            ),
        ),
    ),  # Test functions in conditions
    (
        [["primary_hash", "LIKE", "%foo%"]],
        FunctionCall(
            None,
            ConditionFunctions.LIKE,
            (Column(None, None, "primary_hash"), Literal(None, "%foo%")),
        ),
    ),  # Test output format of LIKE
    (
        [[["notEmpty", ["arrayElement", ["exception_stacks.type", 1]]], "=", 1]],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (
                FunctionCall(
                    None,
                    "notEmpty",
                    (
                        FunctionCall(
                            None,
                            "arrayElement",
                            (
                                Column(None, None, "exception_stacks.type",),
                                Literal(None, 1),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
    ),
    (
        [["exception_frames.filename", "LIKE", "%foo%"]],
        FunctionCall(
            None,
            "arrayExists",
            (
                Lambda(
                    None,
                    ("x",),
                    FunctionCall(
                        None,
                        "assumeNotNull",
                        (
                            FunctionCall(
                                None,
                                ConditionFunctions.LIKE,
                                (Argument(None, "x"), Literal(None, "%foo%")),
                            ),
                        ),
                    ),
                ),
                Column(None, None, "exception_frames.filename"),
            ),
        ),
    ),  # Test scalar condition on array column is expanded as an iterator.
    (
        [["exception_frames.filename", "NOT LIKE", "%foo%"]],
        FunctionCall(
            None,
            "arrayAll",
            (
                Lambda(
                    None,
                    ("x",),
                    FunctionCall(
                        None,
                        "assumeNotNull",
                        (
                            FunctionCall(
                                None,
                                ConditionFunctions.NOT_LIKE,
                                (Argument(None, "x"), Literal(None, "%foo%")),
                            ),
                        ),
                    ),
                ),
                Column(None, None, "exception_frames.filename"),
            ),
        ),
    ),  # Test negative scalar condition on array column is expanded as an all() type iterator.
    (
        [[["equals", ["exception_frames.filename", "'foo'"]], "=", 1]],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (
                FunctionCall(
                    None,
                    "arrayExists",
                    (
                        Lambda(
                            None,
                            ("x",),
                            FunctionCall(
                                None,
                                "assumeNotNull",
                                (
                                    FunctionCall(
                                        None,
                                        ConditionFunctions.EQ,
                                        (Argument(None, "x"), Literal(None, "foo")),
                                    ),
                                ),
                            ),
                        ),
                        Column(None, None, "exception_frames.filename"),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
    ),  # Test function condition on array column is expanded as an iterator.
    (
        [[["notEquals", ["exception_frames.filename", "'foo'"]], "=", 1]],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (
                FunctionCall(
                    None,
                    "arrayAll",
                    (
                        Lambda(
                            None,
                            ("x",),
                            FunctionCall(
                                None,
                                "assumeNotNull",
                                (
                                    FunctionCall(
                                        None,
                                        ConditionFunctions.NEQ,
                                        (Argument(None, "x"), Literal(None, "foo")),
                                    ),
                                ),
                            ),
                        ),
                        Column(None, None, "exception_frames.filename"),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
    ),  # Test negative function condition on array column is expanded as an all() type iterator.
    (
        [
            [
                [
                    "or",
                    [
                        ["equals", ["exception_frames.filename", "'foo'"]],
                        ["equals", ["exception_frames.filename", "'bar'"]],
                    ],
                ],
                "=",
                1,
            ]
        ],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (
                FunctionCall(
                    None,
                    "or",
                    (
                        FunctionCall(
                            None,
                            "arrayExists",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                ConditionFunctions.EQ,
                                                (
                                                    Argument(None, "x"),
                                                    Literal(None, "foo"),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column(None, None, "exception_frames.filename"),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "arrayExists",
                            (
                                Lambda(
                                    None,
                                    ("x",),
                                    FunctionCall(
                                        None,
                                        "assumeNotNull",
                                        (
                                            FunctionCall(
                                                None,
                                                ConditionFunctions.EQ,
                                                (
                                                    Argument(None, "x"),
                                                    Literal(None, "bar"),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                                Column(None, None, "exception_frames.filename"),
                            ),
                        ),
                    ),
                ),
                Literal(None, 1),
            ),
        ),
    ),  # Test array columns in boolean functions are expanded as an iterator.
    (
        [["tags.key", "=", "key"]],
        FunctionCall(
            None,
            ConditionFunctions.EQ,
            (Column(None, None, "tags.key"), Literal(None, "key")),
        ),
    ),  # Array columns not expanded because in arrayjoin
    (
        tuplify(
            [["platform", "IN", ["a", "b", "c"]], ["platform", "IN", ["c", "b", "a"]]]
        ),
        FunctionCall(
            None,
            ConditionFunctions.IN,
            (
                Column(None, None, "platform"),
                FunctionCall(
                    None,
                    "tuple",
                    (Literal(None, "a"), Literal(None, "b"), Literal(None, "c")),
                ),
            ),
        ),
    ),  # Test that a duplicate IN condition is de-duplicated even if the lists are in different orders.
    (
        [["group_id", "IS NULL", None]],
        FunctionCall(
            None, ConditionFunctions.IS_NULL, (Column(None, None, "group_id"),),
        ),
    ),  # Unary condition.
]


@pytest.mark.parametrize("conditions, expected", test_conditions)
def test_conditions_expr(conditions: Sequence[Any], expected: Expression) -> None:
    dataset = get_dataset("events")
    assert parse_conditions_to_expr(conditions, dataset, {"tags.key"}) == expected, str(
        conditions
    )


def test_invalid_conditions() -> None:
    dataset = get_dataset("events")
    is_null = [["group_id", "IS NULL", "I am not valid"]]
    with pytest.raises(Exception):
        parse_conditions_to_expr(is_null, dataset, set())

    binary = [["group_id", "=", None]]
    with pytest.raises(Exception):
        parse_conditions_to_expr(binary, dataset, set())
