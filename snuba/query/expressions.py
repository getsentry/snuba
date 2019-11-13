from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterator, Optional, Sequence

from snuba.query.collections import NodeContainer


@dataclass
class Expression(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST), literals, etc.

    The root of the tree is not a Node itself yet (since it is the Query object).
    Representing the root as a node itself does not seem very useful right now
    since we never traverse the full tree. We could revisit that later.

    All expression can have an optional alias.
    """

    alias: Optional[str]

    @abstractmethod
    def format(self) -> str:
        """
        Turn this node into a string for the Clickhouse query.

        TODO: provide a clickhouse formatter to this method so that, through
        a strategy pattern, this class will provide the content for the query
        and the clickhouse formatter will provide the format.
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms this expression through the function passed in input.
        This works almost like a map function over sequences though, contrarily to
        sequences, this acts on a subtree. The semantics of transform can be different
        between intermediate nodes and leaves, so each node class can implement it
        its own way.

        It returns the new expression. It does not guarantee the returned value
        will be a new object. No guarantee of immutability is given on expressions.
        """
        raise NotImplementedError


class HierarchicalExpression(Expression, NodeContainer[Expression]):
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
        self._set_children(
            list(map(lambda child: child.transform(func), self._get_children()))
        )
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        The order here is arbitrary, postfix is chosen to follow the same
        order we have in the transform method.
        """
        for child in self._get_children():
            if isinstance(child, HierarchicalExpression):
                for sub in child:
                    yield sub
            else:
                yield child
        yield self

    @abstractmethod
    def _get_children(self) -> Sequence[Expression]:
        """
        To be implemented by the subclasses to provide the list of children
        for iteration/transformation.
        """
        raise NotImplementedError

    @abstractmethod
    def _set_children(self, children: Sequence[Expression]) -> None:
        """
        To be implemented by the subclasses to reset the list of children
        for iteration/transformation.
        """
        raise NotImplementedError


@dataclass
class Null(Expression):
    """
    SQL NULL
    """

    def format(self) -> str:
        raise NotImplementedError

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return self


@dataclass
class Literal(Expression):
    """
    A literal in the SQL expression
    """
    value: str

    def format(self) -> str:
        raise NotImplementedError

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)


@dataclass
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """
    column_name: str
    table_name: Optional[str]

    def format(self) -> str:
        raise NotImplementedError

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)


@dataclass
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

    def format(self) -> str:
        raise NotImplementedError

    def _get_children(self) -> Sequence[Expression]:
        return self.parameters

    def _set_children(self, children: Sequence[Expression]) -> None:
        self.parameters = children


@dataclass
class Aggregation(HierarchicalExpression):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """
    function_name: str
    parameters: Sequence[Expression]

    def format(self) -> str:
        raise NotImplementedError

    def _get_children(self) -> Sequence[Expression]:
        return self.parameters

    def _set_children(self, children: Sequence[Expression]) -> None:
        self.parameters = children


class OrderByDirection(Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass
class OrderBy(HierarchicalExpression):
    direction: OrderByDirection
    node: Expression

    def format(self) -> str:
        raise NotImplementedError

    def _get_children(self) -> Sequence[Expression]:
        return [self.node]

    def _set_children(self, children: Sequence[Expression]) -> None:
        assert len(children) == 1
        self.node = children[0]
