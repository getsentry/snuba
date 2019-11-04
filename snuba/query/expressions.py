from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Iterable, Optional, Sequence

from snuba.query.nodes import AliasedNode, Node


class Expression(AliasedNode, ABC):
    """
    Abstract representation of a Query node that can be evaluated as a column.
    This can be a simple column or a nested expression, but not a condition.

    TODO: Support a filter method
    """
    @abstractmethod
    def map(self, closure: Callable[[Expression], Expression]) -> Expression:
        """
        Applies the provided function to this object (and potentially to its
        children when present) and returns the Expression, the callsite should
        replace this one with.

        The closure takes in an Expression and returns an Expression to replace it
        like for a map function over collections.

        Some specific corner case are defined in the subclasses.
        """
        raise NotImplementedError


class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Expression], Expression]) -> Expression:
        return closure(self)

    def iterate(self) -> Iterable[Expression]:
        yield self

    def __init__(self,
        alias: Optional[str],
        column_name: str,
        table_name: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__column_name = column_name
        self.__table_name = table_name

    def get_column_name(self) -> str:
        return self.__column_name

    def get_table_name(self) -> Optional[str]:
        return self.__table_name


class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse
    """

    def __init__(self,
        alias: Optional[str],
        function_name: str,
        parameters: Sequence[Expression],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Expression], Expression]) -> Expression:
        """
        For functions map first processes itself. parameters are processed only
        if mapping itself does not yield a change. If calling map on self return
        a different object (so asking for a replacement) iterating over the previous
        list of parameters may make no sense.
        """
        mapped_function = closure(self)
        if mapped_function == self:
            self.__parameters = map(closure, self.__parameters)
        return mapped_function

    def iterate(self) -> Iterable[Expression]:
        yield self
        for p in self.__parameters:
            for element in p.iterate():
                yield element

    def get_function_name(self) -> str:
        return self.__function_name

    def get_parameters(self) -> Sequence[Expression]:
        return self.__parameters


class Aggregation(AliasedNode):
    """
    Represents an aggregation function to be applied to an expression in the
    current query.

    TODO: I don't think this should exist, but as of now a lot of our query
    processing still relies on aggregation being a first class concept.
    """

    def __init__(
        self,
        function_name: str,
        parameters: Sequence[Expression],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Aggregation], Aggregation]) -> Aggregation:
        raise NotImplementedError

    def iterate(self) -> Iterable[Node]:
        raise NotImplementedError
