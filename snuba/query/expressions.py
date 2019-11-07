from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Iterator

from snuba.query.nodes import AliasedNode
from snuba.query.collections import NodeContainer


class Expression(AliasedNode):
    """
    Abstract representation of a Query node that can be evaluated to a single value.
    This can be a simple column, NULL or a nested expression, but not a condition.
    """
    pass


class ExpressionContainer(NodeContainer[Expression]):
    """
    Container able to iterate over expressions and to map them in place.
    Expressions are trees themselves, so iteration and mapping through this
    container is actually a tree traversal.
    """

    def _iterate_over_children(self,
        children: Iterable[Expression],
    ) -> Iterator[Expression]:
        """
        Traverses the children of a tree node.
        """
        for child in children:
            if isinstance(child, ExpressionContainer):
                for sub in child:
                    yield sub
            else:
                yield child

    def _map_children(self,
        children: Iterable[Expression],
        func: Callable[[Expression], Expression],
    ) -> Iterable[Expression]:
        """
        Maps the children of a tree node.
        """
        def process_child(param: Expression) -> Expression:
            r = func(param)
            if r == param and isinstance(r, ExpressionContainer):
                # The expression was not replaced by the function, which
                # means it was unchanged. This means we need to traverse
                # its children.
                r.map(func)
            return r

        return map(process_child, children)


class Null(Expression):
    def _format_impl(self) -> str:
        # TODO: Implement this
        raise NotImplementedError


@dataclass
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """
    column_name: str
    table_name: Optional[str]

    def _format_impl(self) -> str:
        # TODO: Implement this
        raise NotImplementedError


@dataclass
class FunctionCall(Expression, ExpressionContainer):
    """
    Represents an expression that resolves to a function call on Clickhouse
    """
    function_name: str
    parameters: Iterable[Expression]

    def _format_impl(self) -> str:
        # TODO: Implement this
        raise NotImplementedError

    def map(self, func: Callable[[Expression], Expression]) -> None:
        """
        The children of a FunctionCall are the parameters of the function.
        Thus map runs func on the parameters, not on the function itself.
        """
        self.parameters = self._map_children(self.parameters, func)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a prefix order.
        """
        yield self
        for e in self._iterate_over_children(self.parameters):
            yield e


@dataclass
class Aggregation(AliasedNode, ExpressionContainer):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """
    function_name: str
    parameters: Iterable[Expression]

    def _format_impl(self) -> str:
        # TODO: Implement this
        raise NotImplementedError

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverses the subtrees represented by the parameters of the
        aggregation.
        """
        for e in self._iterate_over_children(self.parameters):
            yield e

    def map(self, func: Callable[[Expression], Expression]) -> None:
        """
        The children of an aggregation are the parameters of the aggregation
        function. This runs the mapping function over them.
        """
        self.parameters = self._map_children(self.parameters, func)
