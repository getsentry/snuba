from __future__ import annotations

from typing import Callable, Iterable, Optional, Sequence, Union

from snuba.query import LimitBy, OrderBy, ProcessableQuery, Query, SelectedExpression
from snuba.query.data_source.join import JoinClause
from snuba.query.expressions import Expression, ExpressionVisitor


class CompositeQuery(Query):
    """
    Query class that represents nested or joined queries.

    The from clause can be a processable query, another composite query,
    a join, but it cannot be a simple data source. That would be a
    ProcessableQuery.
    """

    def __init__(
        self,
        from_clause: Optional[
            Union[
                ProcessableQuery,
                CompositeQuery,
                JoinClause,
            ]
        ],
        # TODO: Consider if to remove the defaults and make some of
        # these fields mandatory. This impacts a lot of code so it
        # would be done on its own.
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
    ):
        super().__init__(
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
        self.__from_clause = from_clause
        self.__final = False

    def __repr__(self) -> str:
        from snuba.query.formatters.tracing import format_query

        return "\n".join(format_query(self))

    def get_from_clause(
        self,
    ) -> Union[ProcessableQuery, CompositeQuery, JoinClause]:
        assert self.__from_clause is not None, "Data source has not been provided yet."
        return self.__from_clause

    def set_from_clause(
        self,
        from_clause: Union[
            ProcessableQuery,
            CompositeQuery,
            JoinClause,
        ],
    ) -> None:
        self.__from_clause = from_clause

    def get_final(self) -> bool:
        return self.__final

    def set_final(self, final: bool) -> None:
        self.__final = final

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_from_clause",)
