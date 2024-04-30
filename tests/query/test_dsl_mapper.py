import pytest

from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    condition_pattern,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
    is_any_binary_condition,
    is_condition,
    is_in_condition,
    is_in_condition_pattern,
    is_not_in_condition,
    is_not_in_condition_pattern,
    is_unary_condition,
    unary_condition,
)
from snuba.query.dsl import literals_array, literals_tuple
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import String

from snuba.query.dsl_mapper import registry


tests = [
    pytest.param(
        FunctionCall(
            None,
            "and",
            (
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column(
                            "_snuba_granularity",
                            None,
                            "granularity",
                        ),
                        Literal(None, 3600),
                    ),
                ),
                FunctionCall(
                    None,
                    "in",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, 1),),
                        ),
                    ),
                ),
            ),
        ),
        """and_cond(FunctionCall(alias=None, function_name='equals', parameters=(Column(alias='_snuba_granularity', table_name=None, column_name='granularity'), Literal(alias=None, value=3600))), FunctionCall(alias=None, function_name='in', parameters=(Column(alias='_snuba_project_id', table_name=None, column_name='project_id'), FunctionCall(alias=None, function_name='tuple', parameters=(Literal(alias=None, value=1),)))))""",
    )
]


@pytest.mark.parametrize("condition, expected", tests)
def test_ast_repr(condition: Expression, expected: str) -> None:
    assert registry.ast_repr(condition) == expected
