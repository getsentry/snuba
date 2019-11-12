from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator, Optional, Sequence

from snuba.query.collections import NodeContainer


class Expression(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It also represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST).

    The root of the tree is not a Node itself yet (since it is the Query object).
    Representing the root as a node itself does not seem very useful right now
    since we never traverse the full tree. We could revisit that later.
    """

    @abstractmethod
    def format(self) -> str:
        """
        Turn this node into a string for the Clickhouse query.

        TODO: provide a clickhouse formatter to this method so that, through
        a strategy pattern, this class will provide the content for the query
        and the clickhouse formatter will provide the format.
        """
        raise NotImplementedError


class ExpressionContainer(NodeContainer[Expression]):
    """
    Container able to iterate over expressions and to transform them in place.
    This class exists for two reasons:
    - in some place we check the type of an object through isinstance, and, since
      the parameter of a generic disappears at runtime we cannot do something like
      isinstance(a, NodeContainer[Expression])
    - provides some common feature to transform and iterate over the children of
      a hierarchical expression like a tree.
    """

    def _iterate_over_children(self,
        children: Iterable[Expression],
    ) -> Iterator[Expression]:
        """
        Traverses the children of a hierarchical container like a tree.
        """
        for child in children:
            if isinstance(child, ExpressionContainer):
                for sub in child:
                    yield sub
            else:
                yield child

    def _transform_children(self,
        children: Sequence[Expression],
        func: Callable[[Expression], Expression],
    ) -> Sequence[Expression]:
        """
        Transforms in place the children of a hierarchical node by applying
        a mapping function.
        """
        def process_child(param: Expression) -> Expression:
            r = func(param)
            if r == param and isinstance(r, ExpressionContainer):
                # The expression was not replaced by the function, which
                # means it was unchanged. This means we need to traverse
                # its children.
                r.transform(func)
            return r

        return list(map(process_child, children))


@dataclass
class AliasedExpression(Expression, ExpressionContainer):
    """
    Wraps an expression and provides it an alias in the query.
    This is an expression itself, thus it will appear when iterating
    over the query.
    """
    alias: Optional[str]
    node: Expression

    def format(self) -> str:
        raise NotImplementedError

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        """
        Applies the transformation to the aliased node.
        """
        self.node = self._transform_children((self.node,), func)[0]

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverses the wrapped node.
        """
        yield self
        for e in self._iterate_over_children([self.node]):
            yield e


class Null(Expression):
    def format(self) -> str:
        raise NotImplementedError


@dataclass
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """
    column_name: str
    table_name: Optional[str]

    def format(self) -> str:
        raise NotImplementedError


@dataclass
class FunctionCall(Expression, ExpressionContainer):
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

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        """
        The children of a FunctionCall are the parameters of the function.
        Thus map runs func on the parameters, not on the function itself.
        """
        self.parameters = self._transform_children(self.parameters, func)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a prefix order.
        """
        yield self
        for e in self._iterate_over_children(self.parameters):
            yield e


@dataclass
class Aggregation(Expression, ExpressionContainer):
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

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverses the subtrees represented by the parameters of the
        aggregation.
        """
        for e in self._iterate_over_children(self.parameters):
            yield e

    def transform(self, func: Callable[[Expression], Expression]) -> None:
        """
        The children of an aggregation are the parameters of the aggregation
        function. This runs the mapping function over them.
        """
        self.parameters = self._transform_children(self.parameters, func)
