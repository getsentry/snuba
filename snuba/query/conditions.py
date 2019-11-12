from snuba.query.expressions import Expression, FunctionCall


class ComparisonsFunctions:
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


class BooleanFunctions:
    """
    Same as comparison functions but for boolean operators.
    """
    NOT = "not"
    AND = "and"
    OR = "or"


def binary_condition(function_name: str, lhs: Expression, rhs: Expression) -> FunctionCall:
    return FunctionCall(function_name, [lhs, rhs])


def unary_condition(function_name: str, operand: Expression) -> FunctionCall:
    return FunctionCall(function_name, [operand])
