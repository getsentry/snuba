from typing import Mapping, Optional, Sequence, Set

from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Expression, FunctionCall, Literal
from snuba.query.matchers import AnyExpression, MatchResult
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Integer
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Or, Param, Pattern, String


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
    function: str, lhs: Expression, rhs: Sequence[Literal]
) -> Expression:
    return binary_condition(function, lhs, literals_tuple(None, rhs))


def __set_condition_pattern(
    lhs: Pattern[Expression], operator: str
) -> FunctionCallPattern:
    return FunctionCallPattern(
        String(operator),
        (
            Param("lhs", lhs),
            Param("tuple", FunctionCallPattern(String("tuple"), None)),
        ),
    )


set_condition_pattern = {
    op: __set_condition_pattern(AnyExpression(), op) for op in FUNCTION_TO_OPERATOR
}


def __is_set_condition(exp: Expression, operator: str) -> bool:
    if is_any_binary_condition(exp, operator):
        if operator in set_condition_pattern:
            if set_condition_pattern[operator].match(exp) is not None:
                assert isinstance(exp, FunctionCall)  # mypy
                assert isinstance(exp.parameters[1], FunctionCall)  # mypy
                # Matchers can't currently match arbitrary numbers of parameters, so test this directly
                return all(isinstance(c, Literal) for c in exp.parameters[1].parameters)

    return False


def in_condition(lhs: Expression, rhs: Sequence[Literal],) -> Expression:
    return __set_condition(ConditionFunctions.IN, lhs, rhs)


def is_in_condition(exp: Expression) -> bool:
    return __is_set_condition(exp, ConditionFunctions.IN)


def is_in_condition_pattern(lhs: Pattern[Expression]) -> FunctionCallPattern:
    return __set_condition_pattern(lhs, ConditionFunctions.IN)


def not_in_condition(lhs: Expression, rhs: Sequence[Literal],) -> Expression:
    return __set_condition(ConditionFunctions.NOT_IN, lhs, rhs,)


def is_not_in_condition(exp: Expression) -> bool:
    return __is_set_condition(exp, ConditionFunctions.NOT_IN)


def is_not_in_condition_pattern(lhs: Pattern[Expression]) -> FunctionCallPattern:
    return __set_condition_pattern(lhs, ConditionFunctions.NOT_IN)


def binary_condition(
    function_name: str, lhs: Expression, rhs: Expression
) -> FunctionCall:
    return FunctionCall(None, function_name, (lhs, rhs))


binary_condition_patterns = {
    op: FunctionCallPattern(String(op), (AnyExpression(), AnyExpression()))
    for op in BINARY_OPERATORS
}


def match_condition(
    exp: Expression,
    operators: Set[str],
    lhs_pattern: Pattern[Expression],
    rhs_pattern: Pattern[Expression],
    commutative: bool,
) -> Optional[MatchResult]:
    """
    Matches a binary condition given the two operands and the valid
    operators. It also supports commutative conditions.
    """
    pattern: Pattern[Expression]
    if commutative:
        pattern = Or(
            [
                FunctionCallPattern(
                    Or([String(op) for op in operators]), (lhs_pattern, rhs_pattern)
                ),
                FunctionCallPattern(
                    Or([String(op) for op in operators]), (rhs_pattern, lhs_pattern)
                ),
            ]
        )
    else:
        pattern = FunctionCallPattern(
            Or([String(op) for op in operators]), (lhs_pattern, rhs_pattern)
        )
    return pattern.match(exp)


def is_any_binary_condition(exp: Expression, operator: str) -> bool:
    if operator in binary_condition_patterns:
        return binary_condition_patterns[operator].match(exp) is not None

    return False


def unary_condition(function_name: str, operand: Expression) -> FunctionCall:
    return FunctionCall(None, function_name, (operand,))


unary_condition_patterns = {
    op: FunctionCallPattern(String(op), (AnyExpression(),)) for op in UNARY_OPERATORS
}


def is_unary_condition(exp: Expression, operator: str) -> bool:
    if operator in unary_condition_patterns:
        return unary_condition_patterns[operator].match(exp) is not None

    return False


def get_first_level_and_conditions(condition: Expression) -> Sequence[Expression]:
    return _get_first_level_conditions(condition, BooleanFunctions.AND)


def get_first_level_or_conditions(condition: Expression) -> Sequence[Expression]:
    return _get_first_level_conditions(condition, BooleanFunctions.OR)


TOP_LEVEL_CONDITIONS = {
    func_name: Or(
        [
            FunctionCallPattern(
                String(func_name),
                (Param("left", AnyExpression()), Param("right", AnyExpression())),
            ),
            FunctionCallPattern(
                String("equals"),
                (
                    FunctionCallPattern(
                        String(func_name),
                        (
                            Param("left", AnyExpression()),
                            Param("right", AnyExpression()),
                        ),
                    ),
                    LiteralPattern(Integer(1)),
                ),
            ),
        ]
    )
    for func_name in [BooleanFunctions.AND, BooleanFunctions.OR]
}


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
    match = TOP_LEVEL_CONDITIONS[function].match(condition)
    if match is not None:
        return [
            *_get_first_level_conditions(match.expression("left"), function),
            *_get_first_level_conditions(match.expression("right"), function),
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
        function, conditions[0], _combine_conditions(conditions[1:], function)
    )


CONDITION_MATCH = Or(
    [
        FunctionCallPattern(
            Or([String(op) for op in BINARY_OPERATORS]),
            (AnyExpression(), AnyExpression()),
        ),
        FunctionCallPattern(
            Or([String(op) for op in UNARY_OPERATORS]), (AnyExpression(),)
        ),
    ]
)


def is_condition(exp: Expression) -> bool:
    return CONDITION_MATCH.match(exp) is not None
