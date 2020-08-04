from typing import Mapping, Optional, Sequence

from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import AnyExpression, Or, Param, Pattern, String


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


UNARY_OPERATORS = [
    ConditionFunctions.IS_NULL,
    ConditionFunctions.IS_NOT_NULL,
]


BINARY_OPERATORS = [
    opr for opr in FUNCTION_TO_OPERATOR if opr not in (set(UNARY_OPERATORS))
] + [BooleanFunctions.AND, BooleanFunctions.OR, BooleanFunctions.NOT]


def __set_condition(
    alias: Optional[str], function: str, lhs: Expression, rhs: Sequence[Literal]
) -> Expression:
    return binary_condition(alias, function, lhs, literals_tuple(None, rhs))


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


set_condition_pattern = {
    op: __set_condition_pattern(AnyExpression(), op) for op in FUNCTION_TO_OPERATOR
}


def __is_set_condition(exp: Expression, operator: str) -> bool:
    if is_binary_condition(exp, operator):
        if operator in set_condition_pattern:
            if set_condition_pattern[operator].match(exp) is not None:
                assert isinstance(exp, FunctionCall)  # mypy
                assert isinstance(exp.parameters[1], FunctionCall)  # mypy
                # Matchers can't currently match arbitrary numbers of parameters, so test this directly
                return all(isinstance(c, Literal) for c in exp.parameters[1].parameters)

    return False


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


def is_not_in_condition_pattern(lhs: Pattern[Expression]) -> FunctionCallPattern:
    return __set_condition_pattern(lhs, ConditionFunctions.NOT_IN)


def binary_condition(
    alias: Optional[str], function_name: str, lhs: Expression, rhs: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (lhs, rhs))


binary_condition_patterns = {
    op: FunctionCallPattern(None, String(op), (AnyExpression(), AnyExpression()))
    for op in BINARY_OPERATORS
}


def is_binary_condition(exp: Expression, operator: str) -> bool:
    if operator in binary_condition_patterns:
        return binary_condition_patterns[operator].match(exp) is not None

    return False


def unary_condition(
    alias: Optional[str], function_name: str, operand: Expression
) -> FunctionCall:
    return FunctionCall(alias, function_name, (operand,))


unary_condition_patterns = {
    op: FunctionCallPattern(None, String(op), (AnyExpression(),))
    for op in UNARY_OPERATORS
}


def is_unary_condition(exp: Expression, operator: str) -> bool:
    if operator in unary_condition_patterns:
        return unary_condition_patterns[operator].match(exp) is not None

    return False


def get_first_level_and_conditions(condition: Expression) -> Sequence[Expression]:
    return _get_first_level_conditions(condition, BooleanFunctions.AND)


def get_first_level_or_conditions(condition: Expression) -> Sequence[Expression]:
    return _get_first_level_conditions(condition, BooleanFunctions.OR)


def _get_first_level_conditions(
    condition: Expression, function: str
) -> Sequence[Expression]:
    """
    Utility function to implement several conditions related
    functionalities that were trivial with the legacy query
    representation where the top level conditions for a query
    were a simple list of conditions.
    In the AST, the condition is a tree, so we need some additional
    logic to extract the operands of the top level AND condition.
    """
    if isinstance(condition, FunctionCall) and condition.function_name == function:
        return [
            *_get_first_level_conditions(condition.parameters[0], function),
            *_get_first_level_conditions(condition.parameters[1], function),
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
    This is the inverse of get_first_level_conditions.
    """

    # TODO: Make BooleanFunctions an enum for stricter typing.
    assert function in (BooleanFunctions.AND, BooleanFunctions.OR)
    assert len(conditions) > 0
    if len(conditions) == 1:
        return conditions[0]

    return binary_condition(
        None, function, conditions[0], _combine_conditions(conditions[1:], function)
    )


CONDITION_MATCH = Or(
    [
        FunctionCallPattern(
            None,
            Or([String(op) for op in BINARY_OPERATORS]),
            (AnyExpression(), AnyExpression()),
        ),
        FunctionCallPattern(
            None, Or([String(op) for op in UNARY_OPERATORS]), (AnyExpression(),)
        ),
    ]
)


def is_condition(exp: Expression) -> bool:
    return CONDITION_MATCH.match(exp) is not None
