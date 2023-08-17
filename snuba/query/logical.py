from __future__ import annotations

from typing import Callable, Iterable, Optional, Sequence

from snuba.query import LimitBy, OrderBy
from snuba.query import ProcessableQuery as AbstractQuery
from snuba.query import SelectionType
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Expression, ExpressionVisitor


class Query(AbstractQuery[Entity]):
    """
    Represents the logical query during query processing.
    This means the query class used between parsing and query translation.

    TODO: This query is supposed to rely on entities as data source.
    This will happen in a following step.
    """

    def __init__(
        self,
        from_clause: Optional[Entity],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[SelectionType] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        sample: Optional[float] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
    ):
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__final = False
        self.__sample = sample

        super().__init__(
            from_clause=from_clause,
            selected_columns=selected_columns,
            array_join=array_join,
            condition=condition,
            groupby=groupby,
            having=having,
            order_by=order_by,
            limitby=limitby,
            limit=limit,
            offset=offset,
            totals=totals,
            granularity=granularity,
        )

    def get_final(self) -> bool:
        return self.__final

    def set_final(self, final: bool) -> None:
        self.__final = final

    def get_sample(self) -> Optional[float]:
        return self.__sample

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_final", "get_sample")

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass
