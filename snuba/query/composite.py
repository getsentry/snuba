from __future__ import annotations

from abc import ABC
from typing import Callable, Iterable

from snuba.query import Query
from snuba.query.expressions import Expression, ExpressionVisitor


class CompositeQuery(Query, ABC):
    """
    Query class that represents nested or joined queries.

    The from clause can be a processable query, another composite query,
    a join, but it cannot be a simple data source. That would be a
    ProcessableQuery.
    """

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass

    def __eq__(self, other: object) -> bool:
        if not super().__eq__(other):
            return False

        assert isinstance(other, type(self))
        return self.get_from_clause() == other.get_from_clause()
