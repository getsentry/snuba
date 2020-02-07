from typing import Optional, Sequence

from snuba.query.expressions import Expression, FunctionCall, Literal

# Add here functions (only stateless stuff) used to make the AST less
# verbose to build.


def literals_tuple(alias: Optional[str], literals: Sequence[Literal]) -> FunctionCall:
    return FunctionCall(alias, "tuple", tuple(literals))


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
