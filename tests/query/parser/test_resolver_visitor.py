from typing import Mapping

import pytest

from snuba import state
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.parser import AliasExpanderVisitor
from snuba.query.parser.exceptions import CyclicAliasException

TEST_CASES = [
    pytest.param(
        Column(alias="a", table_name=None, column_name="a"),
        {"b": FunctionCall(alias="b", function_name="f", parameters=tuple())},
        False,
        Column(alias="a", table_name=None, column_name="a"),
        id="Simple Column - do nothing",
    ),
    pytest.param(
        Literal(alias=None, value="a"),
        {},
        False,
        Literal(alias=None, value="a"),
        id="Simple Literal - do nothing",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="ref"),
        {"ref": FunctionCall(alias="ref", function_name="f", parameters=tuple())},
        False,
        FunctionCall(alias="ref", function_name="f", parameters=tuple()),
        id="Alias resolves to a simple function",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="group_id"),
        {
            "group_id": FunctionCall(
                "group_id",
                function_name="f",
                parameters=(
                    Column(alias=None, table_name=None, column_name="group_id"),
                ),
            )
        },
        False,
        FunctionCall(
            alias="group_id",
            function_name="f",
            parameters=(Column(alias=None, table_name=None, column_name="group_id"),),
        ),
        id="Function replaces column. Inner column not changed",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="group_id"),
        {
            "group_id": FunctionCall(
                alias="group_id",
                function_name="f",
                parameters=(
                    Column(alias=None, table_name=None, column_name="group_id"),
                    FunctionCall(
                        alias=None,
                        function_name="g",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="group_id"),
                        ),
                    ),
                ),
            )
        },
        False,
        FunctionCall(
            alias="group_id",
            function_name="f",
            parameters=(
                Column(alias=None, table_name=None, column_name="group_id"),
                FunctionCall(
                    alias=None,
                    function_name="g",
                    parameters=(
                        Column(alias=None, table_name=None, column_name="group_id"),
                    ),
                ),
            ),
        ),
        id="Function replaces columns nested. Inner column not changed",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="group_id"),
        {
            "group_id": FunctionCall(
                alias="group_id",
                function_name="f",
                parameters=(Column(alias=None, table_name=None, column_name="a"),),
            ),
            "a": FunctionCall(
                alias="a",
                function_name="g",
                parameters=(Column(alias=None, table_name=None, column_name="b"),),
            ),
        },
        True,
        FunctionCall(
            "group_id",
            "f",
            (
                FunctionCall(
                    alias="a",
                    function_name="g",
                    parameters=(Column(alias=None, table_name=None, column_name="b"),),
                ),
            ),
        ),
        id="Nested multi-level aliases fully unpacked",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="a"),
        {
            "a": FunctionCall(
                alias="a",
                function_name="f",
                parameters=(Column(alias=None, table_name=None, column_name="b"),),
            ),
            "b": FunctionCall(
                alias="b",
                function_name="g",
                parameters=(Column(alias=None, table_name=None, column_name="b"),),
            ),
        },
        True,
        FunctionCall(
            alias="a",
            function_name="f",
            parameters=(
                FunctionCall(
                    alias="b",
                    function_name="g",
                    parameters=(Column(alias=None, table_name=None, column_name="b"),),
                ),
            ),
        ),
        id="Function shadows inner column",
    ),
    pytest.param(
        CurriedFunctionCall(
            alias=None,
            internal_function=FunctionCall(
                alias=None, function_name="f", parameters=tuple()
            ),
            parameters=(Column(alias=None, table_name=None, column_name="a"),),
        ),
        {
            "a": FunctionCall(
                alias="a",
                function_name="f",
                parameters=(Column(alias="b", table_name=None, column_name="b"),),
            )
        },
        False,
        CurriedFunctionCall(
            alias=None,
            internal_function=FunctionCall(
                alias=None, function_name="f", parameters=tuple()
            ),
            parameters=(
                FunctionCall(
                    alias="a",
                    function_name="f",
                    parameters=(Column(alias="b", table_name=None, column_name="b"),),
                ),
            ),
        ),
        id="Curried with parameter to expand",
    ),
    pytest.param(
        Column(alias=None, table_name=None, column_name="a"),
        {
            "a": Lambda(
                alias="a",
                parameters=tuple(),
                transformation=FunctionCall(
                    alias="b",
                    function_name="f",
                    parameters=(Column(alias=None, table_name=None, column_name="c"),),
                ),
            ),
            "c": Column(alias="c", table_name=None, column_name="x"),
        },
        True,
        Lambda(
            alias="a",
            parameters=tuple(),
            transformation=FunctionCall(
                alias="b",
                function_name="f",
                parameters=(Column(alias="c", table_name=None, column_name="x"),),
            ),
        ),
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
    state.set_config("query_parsing_expand_aliases", 1)
    assert (
        expression.accept(AliasExpanderVisitor(lookup, [], nested_resolution))
        == expected
    )


def test_circular_dependency() -> None:
    state.set_config("query_parsing_expand_aliases", 1)
    with pytest.raises(CyclicAliasException):
        Column(alias=None, table_name=None, column_name="a").accept(
            AliasExpanderVisitor(
                {
                    "a": FunctionCall(
                        alias="a",
                        function_name="f",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="b"),
                        ),
                    ),
                    "b": FunctionCall(
                        alias="b",
                        function_name="g",
                        parameters=(
                            Column(alias=None, table_name=None, column_name="a"),
                        ),
                    ),
                },
                [],
                True,
            )
        )

    with pytest.raises(CyclicAliasException):
        Column(alias=None, table_name=None, column_name="a").accept(
            AliasExpanderVisitor(
                {
                    "a": FunctionCall(
                        alias="a",
                        function_name="f",
                        parameters=(
                            FunctionCall(
                                alias="b",
                                function_name="g",
                                parameters=(
                                    Column(
                                        alias=None, table_name=None, column_name="a"
                                    ),
                                ),
                            ),
                        ),
                    ),
                },
                [],
                True,
            )
        )
