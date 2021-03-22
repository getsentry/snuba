from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Tuple,
    Union,
)

TVisited = TypeVar("TVisited")


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)
class _Expression:
    # TODO: Make it impossible to assign empty string as an alias.
    alias: Optional[str]


class Expression(_Expression, ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST), literals, etc.

    All expressions can have an optional alias.
    """

    @abstractmethod
    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms this expression through the function passed in input.
        This works almost like a map function over sequences though, contrarily to
        sequences, this acts on a subtree. The semantics of transform can be different
        between intermediate nodes and leaves, so each node class can implement it
        its own way.

        All expressions are frozen dataclasses. This means they are immutable and
        format will either return self or a new instance. It cannot transform the
        expression in place.
        """
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[Expression]:
        """
        Used to iterate over this expression and its children. The exact
        semantics depends on the structure of the expression.
        See the implementations for more details.
        """
        raise NotImplementedError

    @abstractmethod
    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        """
        Accepts a visitor class to traverse the tree. The only role of this method is to
        call the right visit method on the visitor object. Requiring the implementation
        to call the method with the right type forces us to keep the visitor interface
        up to date every time we create a new subclass of Expression.
        """
        raise NotImplementedError


class ExpressionVisitor(ABC, Generic[TVisited]):
    """
    Implementation of a Visitor pattern to simplify traversal of the AST while preserving
    the structure and delegating the control of the traversal algorithm to the client.
    This pattern is generally used for evaluation or formatting. While the iteration
    defined above is for stateless use cases where the order of the nodes is not important.

    The original Visitor pattern does not foresee a return type for visit and accept
    methods, instead it relies on having the Visitor class stateful (any side effect a visit method
    could produce has to make changes to the state of the visitor object). This implementation
    allows the Visitor to define a return type which is generic.
    """

    @abstractmethod
    def visit_literal(self, exp: Literal) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_column(self, exp: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_function_call(self, exp: FunctionCall) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_argument(self, exp: Argument) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visit_lambda(self, exp: Lambda) -> TVisited:
        raise NotImplementedError


OptionalScalarType = Union[None, bool, str, float, int, date, datetime]


@dataclass(frozen=True)
class Literal(Expression):
    """
    A literal in the SQL expression
    """

    value: OptionalScalarType

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_literal(self)


@dataclass(frozen=True)
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    table_name: Optional[str]
    column_name: str

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_column(self)


@dataclass(frozen=True)
class SubscriptableReference(Expression):
    """
    Accesses one entry of a subscriptable column (for example key based access on
    a mapping column like tags[key]).

    The only subscriptable column we support now in the query language is a key-value
    mapping, the key is required to be a literal (not any expression) and the subscriptable
    column cannot be the result of an expression itself (func(asd)[key] is not allowed).
    These constraints could be relaxed should we decided to support them in the query language.
    """

    column: Column
    key: Literal

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_subscriptable_reference(self)

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        transformed_col = self.column.transform(func)
        transformed_key = self.key.transform(func)
        if transformed_col != self.column or transformed_key != self.key:
            return func(replace(self, column=transformed_col, key=transformed_key))
        else:
            # Does not instantiate a copy of the this class if the children did
            # not change. This is possible as this dataclasses are frozen.
            return func(self)

    def __iter__(self) -> Iterator[Expression]:
        # Since column is a column and key is a literal and since none of
        # them is a composite expression we would achieve the same result by yielding
        # directly the column and the key instead of iterating over them.
        # We iterate over them so that this would work correctly independently from
        # any future changes on their __iter__ methods as long as they remain Expressions.
        for sub in self.column:
            yield sub
        for sub in self.key:
            yield sub
        yield self


@dataclass(frozen=True)
class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse.
    This class also represent conditions. Since Clickhouse supports both the conventional
    infix notation for condition and the functional one, we converge into one
    representation only in the AST to make query processing easier.
    A query processor would not have to care of processing both functional conditions
    and infix conditions.
    """

    function_name: str
    # This is a tuple with variable size and not a Sequence to enforce it is hashable
    parameters: Tuple[Expression, ...]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms the subtree starting from the children and then applying
        the transformation function to the root.
        This order is chosen to make the semantics of transform more meaningful,
        the transform operation will be performed on the children first (think
        about the parameters of a function call) and then to the node itself.

        The consequence of this is that, if the transformation function replaces
        the root with something else, with different children, we trust the
        transformation function and we do not run that same function over the
        new children.
        """
        transformed_params = tuple(
            map(lambda child: child.transform(func), self.parameters)
        )
        if transformed_params != self.parameters:
            return func(replace(self, parameters=transformed_params))
        else:
            # Does not instantiate a copy of the this class if the children did
            # not change. This is possible as this dataclasses are frozen.
            return func(self)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        The order here is arbitrary, postfix is chosen to follow the same
        order we have in the transform method.
        """
        for child in self.parameters:
            for sub in child:
                yield sub
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_function_call(self)


@dataclass(frozen=True)
class CurriedFunctionCall(Expression):
    """
    This function call represent a function with currying: f(x)(y).
    it means applying the function returned by f(x) to y.
    Clickhouse has a few of these functions, like topK(5)(col).

    We intentionally support only two groups of parameters to avoid an infinite
    number of parameters groups recursively.
    """

    # The function on left side of the expression.
    # for topK this would be topK(5)
    internal_function: FunctionCall
    # The parameters to apply to the result of internal_function.
    # This is a tuple with variable size and not a Sequence to enforce it is hashable
    parameters: Tuple[Expression, ...]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Applies the transformation function to this expression following
        the same policy of FunctionCall. The only difference is that this
        one transforms the internal function before applying the function to the
        parameters.
        """
        replaced_internal = self.internal_function.transform(func)
        replaced_params = tuple(
            map(lambda child: child.transform(func), self.parameters)
        )
        if (
            replaced_internal != self.internal_function
            or replaced_params != self.parameters
        ):
            return func(
                replace(
                    self,
                    internal_function=replaced_internal,
                    parameters=replaced_params,
                )
            )
        else:
            # Does not instantiate a copy of the this class if the children did
            # not change. This is possible as this dataclasses are frozen.
            return func(self)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        """
        for child in self.internal_function:
            yield child
        for child in self.parameters:
            for sub in child:
                yield sub
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_curried_function_call(self)


@dataclass(frozen=True)
class Argument(Expression):
    """
    A bound variable in a lambda expression. This is used to refer to variables
    declared in the lambda expression
    """

    name: str

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_argument(self)


@dataclass(frozen=True)
class Lambda(Expression):
    """
    A lambda expression in the form (x,y,z -> transform(x,y,z))
    """

    # the parameters in the expressions. These are intentionally not expressions
    # since they are variable names and cannot have aliases
    # This is a tuple with variable size and not a Sequence to enforce it is hashable
    parameters: Tuple[str, ...]
    transformation: Expression

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Applies the transformation to the inner expression but not to the parameters
        declaration.
        """
        transformed_expression = self.transformation.transform(func)
        if transformed_expression != self.transformation:
            return func(
                replace(self, transformation=self.transformation.transform(func))
            )
        else:
            # Does not instantiate a copy of the this class if the children did
            # not change. This is possible as this dataclasses are frozen.
            return func(self)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        """
        for child in self.transformation:
            yield child
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_lambda(self)
