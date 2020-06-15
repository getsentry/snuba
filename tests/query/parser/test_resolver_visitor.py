from typing import Mapping

import pytest

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.parser import AliasExpanderVisitor

TEST_CASES = [
    pytest.param(
        Column("a", None, "a"),
        {"b": FunctionCall("b", "f", tuple())},
        False,
        Column("a", None, "a"),
        id="Simple Column - do nothing",
    ),
    pytest.param(
        Literal(None, "a"),
        {},
        False,
        Literal(None, "a"),
        id="Simple Literal - do nothing",
    ),
    pytest.param(
        Column(None, None, "ref"),
        {"ref": FunctionCall("ref", "f", tuple())},
        False,
        FunctionCall("ref", "f", tuple()),
        id="Alias resolves to a simple function",
    ),
    pytest.param(
        Column(None, None, "group_id"),
        {"group_id": FunctionCall("group_id", "f", (Column(None, None, "group_id"),))},
        False,
        FunctionCall("group_id", "f", (Column(None, None, "group_id"),)),
        id="Function replaces column. Inner column not changed",
    ),
    pytest.param(
        Column(None, None, "group_id"),
        {
            "group_id": FunctionCall(
                "group_id",
                "f",
                (
                    Column(None, None, "group_id"),
                    FunctionCall(None, "g", (Column(None, None, "group_id"),)),
                ),
            )
        },
        False,
        FunctionCall(
            "group_id",
            "f",
            (
                Column(None, None, "group_id"),
                FunctionCall(None, "g", (Column(None, None, "group_id"),)),
            ),
        ),
        id="Function replaces columns nested. Inner column not changed",
    ),
    pytest.param(
        Column(None, None, "group_id"),
        {
            "group_id": FunctionCall("group_id", "f", (Column(None, None, "a"),)),
            "a": FunctionCall("a", "g", (Column(None, None, "b"),)),
        },
        True,
        FunctionCall(
            "group_id", "f", (FunctionCall("a", "g", (Column(None, None, "b"),)),),
        ),
        id="Nested multi-level aliases fully unpacked",
    ),
    pytest.param(
        Column(None, None, "a"),
        {
            "a": FunctionCall("a", "f", (Column(None, None, "b"),)),
            "b": FunctionCall("b", "g", (Column(None, None, "b"),)),
        },
        True,
        FunctionCall("a", "f", (FunctionCall("b", "g", (Column(None, None, "b"),)),),),
        id="Funcion shadows inner column",
    ),
    pytest.param(
        CurriedFunctionCall(
            None, FunctionCall(None, "f", tuple()), (Column(None, None, "a"),)
        ),
        {"a": FunctionCall("a", "f", (Column("b", None, "b"),))},
        False,
        CurriedFunctionCall(
            None,
            FunctionCall(None, "f", tuple()),
            (FunctionCall("a", "f", (Column("b", None, "b"),)),),
        ),
        id="Curried with parameter to expand",
    ),
    pytest.param(
        Column(None, None, "a"),
        {
            "a": Lambda(
                "a", tuple(), FunctionCall("b", "f", (Column(None, None, "c"),))
            ),
            "c": Column("c", None, "x"),
        },
        True,
        Lambda("a", tuple(), FunctionCall("b", "f", (Column("c", None, "x"),))),
        id="Expand column into Lambda",
    ),
]


@pytest.mark.parametrize("expression, lookup, nested_resolution, expected", TEST_CASES)
def test_expand_aliases(
    expression: Expression,
    lookup: Mapping[str, Expression],
    nested_resolution: bool,
    expected: Expression,
) -> None:
    assert (
        expression.accept(AliasExpanderVisitor(lookup, [], nested_resolution))
        == expected
    )


def test_circular_dependency() -> None:
    with pytest.raises(AssertionError):
        Column(None, None, "a").accept(
            AliasExpanderVisitor(
                {
                    "a": FunctionCall("a", "f", (Column(None, None, "b"),)),
                    "b": FunctionCall("b", "g", (Column(None, None, "a"),)),
                },
                [],
                True,
            )
        )

    with pytest.raises(AssertionError):
        Column(None, None, "a").accept(
            AliasExpanderVisitor(
                {
                    "a": FunctionCall(
                        "a", "f", (FunctionCall("b", "g", (Column(None, None, "a"),)),)
                    ),
                },
                [],
                True,
            )
        )
