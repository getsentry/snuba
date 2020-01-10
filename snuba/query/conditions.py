from typing import Mapping, Optional, Sequence

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
    return binary_condition(
        alias, function, lhs, FunctionCall(None, "tuple", tuple(rhs)),
    )


def in_condition(
    alias: Optional[str], lhs: Expression, rhs: Sequence[Literal],
) -> Expression:
    return __set_condition(alias, ConditionFunctions.IN, lhs, rhs,)


def not_in_condition(
    alias: Optional[str], lhs: Expression, rhs: Sequence[Literal],
) -> Expression:
    return __set_condition(alias, ConditionFunctions.NOT_IN, lhs, rhs,)


def binary_condition(
    alias: Optional[str], function_name: str, lhs: Expression, rhs: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (lhs, rhs))


def unary_condition(
    alias: Optional[str], function_name: str, operand: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (operand,))
