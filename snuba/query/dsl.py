from __future__ import annotations

from typing import Optional, Sequence

from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    OptionalScalarType,
    SubscriptableReference,
)

# Add here functions (only stateless stuff) used to make the AST less
# verbose to build.


class NestedColumn:
    """Usage:
    tags = NestedColumn("tags")
    assert tags["some_key"] == SubscriptableReference(
        "_snuba_tags[some_key]",
        Column("_snuba_tags"), None, "tags"),
        Literal(None, "some_key")
    )
    """

    def __init__(self, column_name: str, table_name: str | None = None) -> None:
        self.column_name = column_name
        self.table_name = table_name

    def __getitem__(self, key: str) -> SubscriptableReference:
        return SubscriptableReference(
            f"_snuba_{self.column_name}[{key}]",
            Column(f"_snuba_{self.column_name}", self.table_name, self.column_name),
            Literal(None, key),
        )


class _FunctionCall:
    def __init__(self, name: str) -> None:
        self.name = name

    def _arg_to_literal_expr(self, arg: Expression | OptionalScalarType) -> Expression:
        if isinstance(arg, Expression):
            return arg
        return Literal(None, arg)

    def __call__(
        self, *args: Expression | OptionalScalarType, **kwargs: str
    ) -> FunctionCall:
        alias = kwargs.pop("alias", None)
        if kwargs:
            raise ValueError(f"Unsuppored dsl kwargs: {kwargs}")
        transformed_args = [self._arg_to_literal_expr(arg) for arg in args]
        return FunctionCall(alias, self.name, tuple(transformed_args))


class _Functions:
    def __getattr__(self, name: str) -> _FunctionCall:
        return _FunctionCall(name)


"""
Usage:

from snuba.query.dsl import Functions as f
assert f.equals(1, 1, alias="eq") == FunctionCall(
    "eq", "equals" (Literal(None, 1), Literal(None, 1))
)
"""
Functions = _Functions()


def column(
    column_name: str, table_name: str | None = None, alias: str | None = None
) -> Column:
    return Column(alias, table_name, column_name)


def literal(value: OptionalScalarType, alias: str | None = None) -> Literal:
    return Literal(alias, value)


def literals_tuple(alias: Optional[str], literals: Sequence[Literal]) -> FunctionCall:
    return FunctionCall(alias, "tuple", tuple(literals))


def literals_array(alias: Optional[str], literals: Sequence[Literal]) -> FunctionCall:
    return FunctionCall(alias, "array", tuple(literals))


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


def if_in(
    lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return FunctionCall(alias, "in", (lhs, rhs))


# boolean functions
def binary_condition(
    function_name: str, lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return FunctionCall(alias, function_name, (lhs, rhs))


def equals(
    lhs: Expression | OptionalScalarType, rhs: Expression | OptionalScalarType
) -> FunctionCall:
    left = lhs if isinstance(lhs, Expression) else Literal(None, lhs)
    right = rhs if isinstance(rhs, Expression) else Literal(None, rhs)
    return binary_condition("equals", left, right)


def and_cond(lhs: Expression, rhs: Expression, *args: Expression) -> FunctionCall:
    return FunctionCall(None, "and", (lhs, rhs, *args))


def or_cond(lhs: Expression, rhs: Expression, *args: Expression) -> FunctionCall:
    return FunctionCall(None, "or", (lhs, rhs, *args))


def in_cond(
    lhs: Expression, rhs: Expression, alias: Optional[str] = None
) -> FunctionCall:
    return binary_condition("in", lhs, rhs, alias)


# aggregate functions
def count(column: Optional[Column] = None, alias: Optional[str] = None) -> FunctionCall:
    return FunctionCall(alias, "count", (column,) if column else ())


def countIf(
    condition: FunctionCall,
    column: Optional[Column] = None,
    alias: Optional[str] = None,
) -> FunctionCall:
    return FunctionCall(
        alias, "countIf", (condition, column) if column else (condition,)
    )


def identity(expression: Expression, alias: Optional[str]) -> FunctionCall:
    return FunctionCall(alias, "identity", (expression,))
