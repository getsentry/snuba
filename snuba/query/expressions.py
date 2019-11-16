from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Sequence, Union


@dataclass(frozen=True)
class Expression(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST), literals, etc.

    All expressions can have an optional alias.
    """

    alias: Optional[str]

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


class HierarchicalExpression(Expression):
    """
    Expression that represent an intermediate node in the tree, which thus
    can have children.

    It provides two methods to iterate and transform. They both traverse
    the subtree in a postfix order.
    """

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms the subtree starting from the children and then applying
        the transformation function to the root.
        This order is chosen to make the semantics of transform more meaningful,
        the transform operation will be performed on thechildren first (think
        about the parameters of a function call) and then to the node itself.

        The consequence of this is that, if the transformation function replaces
        the root with something else, with different children, we trust the
        transformation function and we do not run that same function over the
        new children.
        """
        transformed = self._duplicate_with_new_children(
            list(map(lambda child: child.transform(func), self._get_children()))
        )
        return func(transformed)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        The order here is arbitrary, postfix is chosen to follow the same
        order we have in the transform method.
        """
        for child in self._get_children():
            for sub in child:
                yield sub
        yield self

    @abstractmethod
    def _get_children(self) -> Sequence[Expression]:
        """
        To be implemented by the subclasses to provide the list of children
        for iteration/transformation.
        """
        raise NotImplementedError

    @abstractmethod
    def _duplicate_with_new_children(self, children: Sequence[Expression]) -> Expression:
        """
        Return a new instance of the expression with a new set of children.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Literal(Expression):
    """
    A literal in the SQL expression
    """
    value: Union[None, bool, str, float, int]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self


@dataclass(frozen=True)
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """
    column_name: str
    table_name: Optional[str]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self


@dataclass(frozen=True)
class FunctionCall(HierarchicalExpression):
    """
    Represents an expression that resolves to a function call on Clickhouse.
    This class also represent conditions. Since Clickhouse supports both the conventional
    infix notation for condition and the functional one, we converge into one
    representation only in the AST to make query processing easier.
    A query processor would not have to care of processing both functional conditions
    and infix conditions.
    """
    function_name: str
    parameters: Sequence[Expression]

    def _get_children(self) -> Sequence[Expression]:
        return self.parameters

    def _duplicate_with_new_children(self, children: Sequence[Expression]) -> Expression:
        return FunctionCall(self.alias, self.function_name, children)


@dataclass(frozen=True)
class Aggregation(HierarchicalExpression):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """
    function_name: str
    parameters: Sequence[Expression]

    def _get_children(self) -> Sequence[Expression]:
        return self.parameters

    def _duplicate_with_new_children(self, children: Sequence[Expression]) -> Expression:
        return Aggregation(self.alias, self.function_name, children)
