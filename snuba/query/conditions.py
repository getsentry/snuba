from typing import Mapping, Optional, Sequence

from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Param, String, Pattern


class ConditionFunctions:
    """
    Function names for comparison operations.
    """

    EQ = "equals"
    NEQ = "notEquals"
    LTE = "lessOrEquals"
    GTE = "greaterOrEquals"
    LT = "less"
    GT = "greater"
    IS_NULL = "isNull"
    IS_NOT_NULL = "isNotNull"
    LIKE = "like"
    NOT_LIKE = "notLike"
    IN = "in"
    NOT_IN = "notIn"


OPERATOR_TO_FUNCTION: Mapping[str, str] = {
    ">": ConditionFunctions.GT,
    "<": ConditionFunctions.LT,
    ">=": ConditionFunctions.GTE,
    "<=": ConditionFunctions.LTE,
    "=": ConditionFunctions.EQ,
    "!=": ConditionFunctions.NEQ,
    "IN": ConditionFunctions.IN,
    "NOT IN": ConditionFunctions.NOT_IN,
    "IS NULL": ConditionFunctions.IS_NULL,
    "IS NOT NULL": ConditionFunctions.IS_NOT_NULL,
    "LIKE": ConditionFunctions.LIKE,
    "NOT LIKE": ConditionFunctions.NOT_LIKE,
}


class BooleanFunctions:
    """
    Same as comparison functions but for boolean operators.
    """

    NOT = "not"
    AND = "and"
    OR = "or"


def __set_condition(
    alias: Optional[str], function: str, lhs: Expression, rhs: Sequence[Literal]
) -> Expression:
    return binary_condition(alias, function, lhs, literals_tuple(None, rhs))


def __is_set_condition(exp: Expression, operator: str) -> bool:
    if is_binary_condition(exp, operator):
        assert isinstance(exp, FunctionCall)
        return (
            isinstance(exp.parameters[1], FunctionCall)
            and exp.parameters[1].function_name == "tuple"
            and all(isinstance(c, Literal) for c in exp.parameters[1].parameters)
        )
    return False


def __set_condition_pattern(
    lhs: Pattern[Expression], operator: str
) -> FunctionCallPattern:
    return FunctionCallPattern(
        None,
        String(operator),
        (
            Param("lhs", lhs),
            Param("tuple", FunctionCallPattern(None, String("tuple"), None)),
        ),
    )


def in_condition(
    alias: Optional[str], lhs: Expression, rhs: Sequence[Literal],
) -> Expression:
    return __set_condition(alias, ConditionFunctions.IN, lhs, rhs,)


def is_in_condition(exp: Expression) -> bool:
    return __is_set_condition(exp, ConditionFunctions.IN)


def is_in_condition_pattern(lhs: Pattern[Expression]) -> FunctionCallPattern:
    return __set_condition_pattern(lhs, ConditionFunctions.IN)


def not_in_condition(
    alias: Optional[str], lhs: Expression, rhs: Sequence[Literal],
) -> Expression:
    return __set_condition(alias, ConditionFunctions.NOT_IN, lhs, rhs,)


def is_not_in_condition(exp: Expression) -> bool:
    return __is_set_condition(exp, ConditionFunctions.NOT_IN)


def binary_condition(
    alias: Optional[str], function_name: str, lhs: Expression, rhs: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (lhs, rhs))


def is_binary_condition(exp: Expression, operator: str) -> bool:
    return (
        isinstance(exp, FunctionCall)
        and exp.function_name == operator
        and len(exp.parameters) == 2
    )


def unary_condition(
    alias: Optional[str], function_name: str, operand: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (operand,))


def is_unary_condition(exp: Expression, operator: str) -> bool:
    return (
        isinstance(exp, FunctionCall)
        and exp.function_name == operator
        and len(exp.parameters) == 1
    )


def get_first_level_conditions(condition: Expression) -> Sequence[Expression]:
    """
    Utility function to implement several conditions related
    functionalities that were trivial with the legacy query
    representation where the top level conditions for a query
    were a simple list of conditions.
    In the AST, the condition is a tree, so we need some additional
    logic to extract the operands of the top level AND condition.
    """
    if (
        isinstance(condition, FunctionCall)
        and condition.function_name == BooleanFunctions.AND
    ):
        return [
            *get_first_level_conditions(condition.parameters[0]),
            *get_first_level_conditions(condition.parameters[1]),
        ]
    else:
        return [condition]


def combine_or_conditions(conditions: Sequence[Expression]) -> Expression:
    return _combine_conditions(conditions, BooleanFunctions.OR)


def combine_and_conditions(conditions: Sequence[Expression]) -> Expression:
    return _combine_conditions(conditions, BooleanFunctions.AND)


def _combine_conditions(conditions: Sequence[Expression], function: str) -> Expression:
    """
    Combine multiple independent conditions in a single function
    representing an AND or an OR.
    This is the inverse of get_first_level_conditions with the
    difference that it can actually combine both ORs and ANDs.
    """

    # TODO: Make BooleanFunctions an enum for stricter typing.
    assert function in (BooleanFunctions.AND, BooleanFunctions.OR)
    assert len(conditions) > 0
    if len(conditions) == 1:
        return conditions[0]

    return binary_condition(
        None, function, conditions[0], _combine_conditions(conditions[1:], function)
    )
