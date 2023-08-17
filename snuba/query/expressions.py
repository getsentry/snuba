from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Callable, Generic, Iterator, Optional, Tuple, TypeVar, Union

from snuba import settings

TVisited = TypeVar("TVisited")
# dataclasses have their own built in repr, we override it
# under usual circumstances to get more readable expressions.

# The dataclass repr is created by the @dataclass decorator at
# class definition time, therefore we have to know up front whether we
# will be using the dataclass repr or not
# Sometimes however, we want the raw data, and this allows us to print that out
_AUTO_REPR = not settings.PRETTY_FORMAT_EXPRESSIONS


# This is a workaround for a mypy bug, found here: https://github.com/python/mypy/issues/5374
@dataclass(frozen=True, repr=_AUTO_REPR)
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

    def __repr__(self) -> str:
        """returns a stringified version of the expression AST that is concise and easy to parse visually.
        Not expected to be used for anything except debugging
        (it does a lot of string copies to construct the string)
        """
        if settings.PRETTY_FORMAT_EXPRESSIONS:
            visitor = StringifyVisitor()
            return self.accept(visitor)
        else:
            return super().__repr__()

    def functional_eq(self, other: Expression) -> bool:
        """Returns if an expression is functionally equivalent to the other. i.e. performs an equality
        operation ignoring aliases
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


class NoopVisitor(ExpressionVisitor[None]):
    """A noop visitor that will traverse every node but will not
    return anything. Good for extending for search purposes
    """

    def visit_literal(self, exp: Literal) -> None:
        return None

    def visit_column(self, exp: Column) -> None:
        return None

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> None:
        return exp.column.accept(self)

    def visit_function_call(self, exp: FunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        return None

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        return exp.internal_function.accept(self)

    def visit_argument(self, exp: Argument) -> None:
        return None

    def visit_lambda(self, exp: Lambda) -> None:
        return exp.transformation.accept(self)


class StringifyVisitor(ExpressionVisitor[str]):
    """Visitor implementation to turn an expression into a string format
    Usage:
        # Any expression class supported by the visitor will do
        >>> exp: Expression = Expression()
        >>> visitor = StringifyVisitor()
        >>> exp_str = exp.accept(visitor)
    """

    def __init__(self, level: int = 0, initial_indent: int = 0) -> None:
        # keeps track of the level of the AST we are currently in,
        # this is necessary for nice indentation

        # before recursively going into subnodes increment this counter,
        # decrement it after the recursion is done
        self.__level = level

        # the initial indent that the repr string should have
        self.__initial_indent = initial_indent

    def _get_line_prefix(self) -> str:
        # every line in the tree needs to be indented based on the tree level
        # to make things look pretty
        return "  " * (self.__initial_indent + self.__level)

    def _get_alias_str(self, exp: Expression) -> str:
        # Every expression has an optional alias so we handle that here
        return f" AS `{exp.alias}`" if exp.alias else ""

    def visit_literal(self, exp: Literal) -> str:
        literal_str = None
        if isinstance(exp.value, str):
            literal_str = f"'{exp.value}'"
        elif isinstance(exp.value, datetime):
            literal_str = f"datetime({exp.value.isoformat()})"
        elif isinstance(exp.value, date):
            literal_str = f"date({exp.value.isoformat()})"
        else:
            literal_str = f"{exp.value}"
        res = f"{self._get_line_prefix()}{literal_str}{self._get_alias_str(exp)}"
        return res

    def visit_column(self, exp: Column) -> str:
        column_str = (
            f"{exp.table_name}.{exp.column_name}"
            if exp.table_name
            else f"{exp.column_name}"
        )
        return f"{self._get_line_prefix()}{column_str}{self._get_alias_str(exp)}"

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        # we want to visit the literal node to format it properly
        # but for the subscritable reference we don't need it to
        # be indented or newlined. Hence we remove the prefix
        # from the string
        literal_str = exp.key.accept(self)[len(self._get_line_prefix()) :]

        # if the subscripted column is aliased, we wrap it with parens to make life
        # easier for the viewer
        column_str = (
            f"({exp.column.accept(self)})"
            if exp.column.alias is not None
            else f"{exp.column.accept(self)}"
        )

        # this line will already have the necessary prefix due to the visit_column
        # function
        subscripted_column_str = f"{column_str}[{literal_str}]"
        # after we know that, all we need to do as add the alias
        return f"{subscripted_column_str}{self._get_alias_str(exp)}"

    def visit_function_call(self, exp: FunctionCall) -> str:
        self.__level += 1
        param_str = ",".join([f"\n{param.accept(self)}" for param in exp.parameters])
        self.__level -= 1
        return f"{self._get_line_prefix()}{exp.function_name}({param_str}\n{self._get_line_prefix()}){self._get_alias_str(exp)}"

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        self.__level += 1
        param_str = ",".join([f"\n{param.accept(self)}" for param in exp.parameters])
        self.__level -= 1
        # The internal function repr will already have the
        # prefix appropriate for the level, we don't need to
        # insert it here
        return f"{exp.internal_function.accept(self)}({param_str}\n{self._get_line_prefix()}){self._get_alias_str(exp)}"

    def visit_argument(self, exp: Argument) -> str:
        return f"{self._get_line_prefix()}{exp.name}{self._get_alias_str(exp)}"

    def visit_lambda(self, exp: Lambda) -> str:
        params_str = ",".join(exp.parameters)
        self.__level += 1
        transformation_str = exp.transformation.accept(self)
        self.__level -= 1
        return f"{self._get_line_prefix()}({params_str} ->\n{transformation_str}\n{self._get_line_prefix()}){self._get_alias_str(exp)}"


OptionalScalarType = Union[None, bool, str, float, int, date, datetime]


@dataclass(frozen=True, repr=_AUTO_REPR)
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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.value == other.value


@dataclass(frozen=True, repr=_AUTO_REPR)
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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.table_name == other.table_name
            and self.column_name == other.column_name
        )


@dataclass(frozen=True, repr=_AUTO_REPR)
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
        transformed = replace(
            self,
            column=self.column.transform(func),
            key=self.key.transform(func),
        )
        return func(transformed)

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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.column.functional_eq(other.column) and self.key.functional_eq(
            other.key
        )


@dataclass(frozen=True, repr=_AUTO_REPR)
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
        transformed = replace(
            self,
            parameters=tuple(map(lambda child: child.transform(func), self.parameters)),
        )
        return func(transformed)

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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.function_name != other.function_name:
            return False
        if len(self.parameters) != len(other.parameters):
            return False
        for i, param in enumerate(self.parameters):
            other_parameter = other.parameters[i]
            params_functionally_equivalent = param.functional_eq(other_parameter)
            if not params_functionally_equivalent:
                return False
        return True


@dataclass(frozen=True, repr=_AUTO_REPR)
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
        transformed = replace(
            self,
            internal_function=self.internal_function.transform(func),
            parameters=tuple(map(lambda child: child.transform(func), self.parameters)),
        )
        return func(transformed)

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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if not self.internal_function.functional_eq(other.internal_function):
            return False
        for i, param in enumerate(self.parameters):
            other_parameter = other.parameters[i]
            params_functionally_equivalent = param.functional_eq(other_parameter)
            if not params_functionally_equivalent:
                return False
        return True


@dataclass(frozen=True, repr=_AUTO_REPR)
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

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.name == other.name


@dataclass(frozen=True, repr=_AUTO_REPR)
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
        transformed = replace(self, transformation=self.transformation.transform(func))
        return func(transformed)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        """
        for child in self.transformation:
            yield child
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visit_lambda(self)

    def functional_eq(self, other: Expression) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.parameters != other.parameters:
            return False
        if not self.transformation.functional_eq(other.transformation):
            return False
        return True
