from __future__ import annotations

from typing import Callable, Generic, Iterable, Optional, Sequence, Union

from snuba.query import (
    Limitby,
    OrderBy,
    ProcessableQuery,
    Query,
    SelectedExpression,
    TSimpleDataSource,
)
from snuba.query.expressions import Expression, ExpressionVisitor


class CompositeQuery(Query, Generic[TSimpleDataSource]):
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
                # TODO: Add the join here when it will exist.
                ProcessableQuery[TSimpleDataSource],
                CompositeQuery[TSimpleDataSource],
            ]
        ],
        # TODO: Consider if to remove the defaults and make some of
        # these fields mandatory. This impacts a lot of code so it
        # would be done on its own.
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Expression] = None,
        condition: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[Limitby] = None,
        sample: Optional[float] = None,
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
            sample=sample,
            limit=limit,
            offset=offset,
            totals=totals,
            granularity=granularity,
        )
        self.__from_clause = from_clause

    def get_from_clause(
        self,
    ) -> Union[ProcessableQuery[TSimpleDataSource], CompositeQuery[TSimpleDataSource]]:
        assert self.__from_clause is not None, "Data source has not been provided yet."
        return self.__from_clause

    def set_from_clause(
        self,
        from_clause: Union[
            ProcessableQuery[TSimpleDataSource], CompositeQuery[TSimpleDataSource]
        ],
    ) -> None:
        self.__from_clause = from_clause

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass
