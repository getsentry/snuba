from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Iterable, Optional, Sequence

from snuba.query.nodes import AliasedNode, Node


class Expression(AliasedNode, ABC):
    """
    Abstract representation of a Query node that can be evaluated as a column.
    This can be a simple column or a nested expression, but not a condition.
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

    @abstractmethod
    def filter(self, closure: Callable[[Expression], bool]) -> bool:
        """
        Applies the provided function to this object (and potentially to its
        children when present) and removes the children where the function returns
        False. If this function returns false, the parent Node will remove this Node.

        It is up to the subclasses to guarantee the expression remains valid.
        """
        raise NotImplementedError


class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Expression], Expression]) -> Expression:
        raise NotImplementedError

    def filter(self, closure: Callable[[Expression], bool]) -> bool:
        raise NotImplementedError

    def iterate(self) -> Iterable[Expression]:
        raise NotImplementedError

    def __init__(self,
        column_name: str,
        table_name: Optional[str],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__column_name = column_name
        self.__table_name = table_name


class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse
    """

    def __init__(self,
        function_name: str,
        parameters: Sequence[Expression],
        alias: Optional[str],
    ) -> None:
        super().__init__(alias=alias)
        self.__function_name = function_name
        self.__parameters = parameters

    def _format_impl(self) -> str:
        raise NotImplementedError

    def map(self, closure: Callable[[Expression], Expression]) -> Expression:
        raise NotImplementedError

    def filter(self, closure: Callable[[Expression], bool]) -> bool:
        raise NotImplementedError

    def iterate(self) -> Iterable[Expression]:
        raise NotImplementedError


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

    def filter(self, closure: Callable[[Aggregation], bool]) -> bool:
        raise NotImplementedError

    def iterate(self) -> Iterable[Node]:
        raise NotImplementedError
