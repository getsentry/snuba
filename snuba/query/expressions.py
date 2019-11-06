from __future__ import annotations

from abc import ABC
from typing import Callable, Iterable, Optional, Iterator

from snuba.query.collections import NodeContainer
from snuba.query.nodes import AliasedNode


class Expression(AliasedNode, ABC):
    """
    Abstract representation of a Query node that can be evaluated to a single value.
    This can be a simple column or a nested expression, but not a condition.
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
        raise NotImplementedError


class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    def __init__(self,
        alias: Optional[str],
        column_name: str,
        table_name: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__column_name = column_name
        self.__table_name = table_name

    def __repr__(self) -> str:
        return f"{self.__table_name}.{self.__column_name} as {self._get_alias()}"

    def _format_impl(self) -> str:
        raise NotImplementedError

    def get_column_name(self) -> str:
        return self.__column_name

    def get_table_name(self) -> Optional[str]:
        return self.__table_name


class FunctionCall(Expression, ExpressionContainer):
    """
    Represents an expression that resolves to a function call on Clickhouse
    """

    def __init__(self,
        alias: Optional[str],
        function_name: str,
        parameters: Iterable[Expression],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters: Iterable[Expression] = parameters

    def __repr__(self) -> str:
        return f"{self.__function_name}({list(self.__parameters)}) as {self._get_alias()}"

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        """
        The children of a FunctionCall are the parameters of the function.
        Thus map runs the closure on the parameters, not on the function
        itself.
        """
        self.__parameters = self._map_children(self.__parameters, closure)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a prefix order.
        """
        yield self
        for e in self._iterate_over_children(self.__parameters):
            yield e

    def get_function_name(self) -> str:
        return self.__function_name

    def get_parameters(self) -> Iterable[Expression]:
        return self.__parameters


class Aggregation(AliasedNode, ExpressionContainer):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """

    def __init__(
        self,
        alias: Optional[str],
        function_name: str,
        parameters: Iterable[Expression],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters

    def _format_impl(self) -> str:
        raise NotImplementedError

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverses the subtrees represented by the parameters of the
        aggregation.
        """
        for e in self._iterate_over_children(self.__parameters):
            yield e

    def map(self, closure: Callable[[Expression], Expression]) -> None:
        """
        The children of an aggregation are the parameters of the aggregation
        function. This runs the mapping closure over them.
        """
        self.__parameters = self._map_children(self.__parameters, closure)
