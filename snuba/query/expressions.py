from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Iterator

from snuba.query.collections import NodeContainer
from snuba.query.nodes import AliasedNode


class Expression(AliasedNode, ABC):
    """
    Abstract representation of a Query node that can be evaluated to a single value.
    This can be a simple column, NULL or a nested expression, but not a condition.
    """
    pass


class ExpressionContainer(NodeContainer[Expression]):
    """
    Container able to iterate and map expression in place.
    This class exists to be able to preserve the NodeContainer[Expression]
    type at runtime.
    """

    def _map_children(self,
        children: Iterable[Expression],
        closure: Callable[[Expression], Expression],
    ) -> Iterable[Expression]:
        def process_child(param: Expression) -> Expression:
            r = closure(param)
            if r == param and isinstance(r, ExpressionContainer):
                # The expression was not replaced by the closure, which
                # means it was unchanged. This means we need to traverse
                # its children.
                r.map(closure)
            return r

        return map(process_child, children)

    def _iterate_over_children(self,
        children: Iterable[Expression],
    ) -> Iterator[Expression]:
        for child in children:
            if isinstance(child, ExpressionContainer):
                for sub in child:
                    yield sub
            else:
                yield child


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

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        """
        The children of a FunctionCall are the parameters of the function.
        Thus map runs the closure on the parameters, not on the function
        itself.
        """
        self.parameters = self._map_children(self.parameters, closure)

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

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        """
        The children of an aggregation are the parameters of the aggregation
        function. This runs the mapping closure over them.
        """
        self.parameters = self._map_children(self.parameters, closure)
