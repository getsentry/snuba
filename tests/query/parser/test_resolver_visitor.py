import pytest

from typing import Mapping, Set

from snuba.query.parser import AliasResolverVisitor
from snuba.query.expressions import (
    Column,
    Expression,
    Literal,
    FunctionCall,
    CurriedFunctionCall,
)


TEST_CASES = [
    pytest.param(
        Column("a", None, "a"),
        {"b": FunctionCall("b", "f", tuple())},
        set(),
        Column("a", None, "a"),
        id="Simple Column - do nothing",
    ),
    pytest.param(
        Literal(None, "a"),
        {},
        set(),
        Literal(None, "a"),
        id="Simple Literal - do nothing",
    ),
    pytest.param(
        Column(None, None, "a"),
        {"a": FunctionCall("a", "f", tuple())},
        {"a"},
        Column(None, None, "a"),
        id="Alias to ignore",
    ),
    pytest.param(
        Column(None, None, "ref"),
        {"ref": FunctionCall("ref", "f", tuple())},
        set(),
        FunctionCall("ref", "f", tuple()),
        id="Alias resolves to a simple function",
    ),
    pytest.param(
        Column(None, None, "group_id"),
        {"group_id": FunctionCall("group_id", "f", (Column(None, None, "group_id"),))},
        set(),
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
        set(),
        FunctionCall(
            "group_id",
            "f",
            (
                Column(None, None, "group_id"),
                FunctionCall(None, "g", (Column(None, None, "group_id"),)),
            ),
        ),
        id="Function replaces columns. Inner column not changed",
    ),
    pytest.param(
        Column(None, None, "group_id"),
        {
            "group_id": FunctionCall("group_id", "f", (Column(None, None, "a"),)),
            "a": FunctionCall("a", "g", (Column(None, None, "b"),)),
        },
        set(),
        FunctionCall(
            "group_id", "f", (FunctionCall("a", "g", (Column(None, None, "b"),)),),
        ),
        id="Nested multi-level aliases",
    ),
    pytest.param(
        CurriedFunctionCall(
            None, FunctionCall(None, "f", tuple()), (Column(None, None, "a"),)
        ),
        {"a": FunctionCall("a", "f", (Column("b", None, "b"),))},
        set(),
        CurriedFunctionCall(
            None,
            FunctionCall(None, "f", tuple()),
            (FunctionCall("a", "f", (Column("b", None, "b"),)),),
        ),
        id="Curried with parameter to expand",
    ),
]


@pytest.mark.parametrize("expression, lookup, to_ignore, expected", TEST_CASES)
def test_format_expressions(
    expression: Expression,
    lookup: Mapping[str, Expression],
    to_ignore: Set[str],
    expected: Expression,
) -> None:
    assert expression.accept(AliasResolverVisitor(lookup, to_ignore)) == expected
