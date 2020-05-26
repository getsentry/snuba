from typing import Mapping, Optional, Sequence

from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Expression, Literal, FunctionCall


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

FUNCTION_TO_OPERATOR: Mapping[str, str] = {
    func: op for op, func in OPERATOR_TO_FUNCTION.items()
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


def in_condition(
    alias: Optional[str], lhs: Expression, rhs: Sequence[Literal],
) -> Expression:
    return __set_condition(alias, ConditionFunctions.IN, lhs, rhs,)


def is_in_condition(exp: Expression) -> bool:
    return __is_set_condition(exp, ConditionFunctions.IN)


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


def is_condition(exp: Expression) -> bool:
    return (
        isinstance(exp, FunctionCall)
        and exp.function_name in FUNCTION_TO_OPERATOR
        and len(exp.parameters) > 0
    )
