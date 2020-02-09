from typing import Optional, Sequence

from snuba.query.expressions import Column, Expression, FunctionCall, Literal

# Add here functions (only stateless stuff) used to make the AST less
# verbose to build.


def literals_tuple(alias: Optional[str], literals: Sequence[Literal]) -> FunctionCall:
    return FunctionCall(alias, "tuple", tuple(literals))


# Array functions
def array_element(
    alias: Optional[str], array_col: Column, index: Expression
) -> FunctionCall:
    return FunctionCall(alias, "arrayElement", (array_col, index))


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


def div(lhs: Expression, rhs: Expression, alias: Optional[str] = None) -> FunctionCall:
    return FunctionCall(alias, "div", (lhs, rhs))
