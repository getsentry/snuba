from typing import Optional, Sequence

from snuba.query.expressions import Expression, FunctionCall, Literal, Column

# Add here functions (only stateless stuff) used to make the AST less
# verbose to build.


def literals_tuple(alias: Optional[str], literals: Sequence[Literal]) -> FunctionCall:
    return FunctionCall(alias, "tuple", tuple(literals))


# Array functions
def arrayElement(
    alias: Optional[str], array: Expression, index: Expression
) -> FunctionCall:
    return FunctionCall(alias, "arrayElement", (array, index))


def arrayJoin(alias: Optional[str], content: Expression) -> Expression:
    return FunctionCall(alias, "arrayJoin", (content,))


# Tuple functions
def tupleElement(
    alias: Optional[str], tuple_expr: Expression, index: Expression
) -> FunctionCall:
    return FunctionCall(alias, "tupleElement", (tuple_expr, index))


# arithmetic function
def plus(lhs: Expression, rhs: Expression, alias: Optional[str] = None) -> FunctionCall:
    return FunctionCall(alias, "plus", (lhs, rhs))


def minus(
    lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return FunctionCall(alias, "minus", (lhs, rhs))


def multiply(
    lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return FunctionCall(alias, "multiply", (lhs, rhs))


def divide(
    lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return FunctionCall(alias, "divide", (lhs, rhs))


# aggregate functions
def count(column: Optional[Column] = None, alias: Optional[str] = None):
    return FunctionCall(alias, "count", (column,) if column else ())


def countIf(
    condition: FunctionCall,
    column: Optional[Column] = None,
    alias: Optional[str] = None,
):
    return FunctionCall(
        alias, "countIf", (condition, column) if column else (condition,)
    )
