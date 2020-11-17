from __future__ import annotations

from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from deprecation import deprecated
from snuba.query import Limitby, OrderBy
from snuba.query import ProcessableQuery as AbstractQuery
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery as AbstractCompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Expression, ExpressionVisitor

Aggregation = Union[
    Tuple[Any, Any, Any], Sequence[Any],
]


class Query(AbstractQuery[Entity]):
    """
    Represents the logical query during query processing.
    This means the query class used between parsing and query translation.

    TODO: This query is supposed to rely on entities as data source.
    This will happen in a following step.
    """

    def __init__(
        self,
        body: MutableMapping[str, Any],  # Temporary
        from_clause: Optional[Entity],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Expression] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
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
        """
        Expects an already parsed query body.
        """
        # TODO: make the parser produce this data structure directly
        # in order not to expose the internal representation.
        self.__body = body
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

    def set_sample(self, final: bool) -> None:
        self.__final = final

    def __eq__(self, other: object) -> bool:
        if not super().__eq__(other):
            return False

        assert isinstance(other, Query)  # mypy
        return (
            self.get_final() == other.get_final()
            and self.get_sample() == other.get_sample()
        )

    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods instead."
    )
    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        pass

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        pass


class CompositeQuery(AbstractCompositeQuery):
    def __init__(
        self,
        from_clause: Optional[Union[Query, CompositeQuery, JoinClause[Entity]]],
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Expression] = None,
        condition: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[Limitby] = None,
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

    def get_from_clause(self) -> Union[Query, CompositeQuery, JoinClause[Entity]]:
        assert self.__from_clause is not None, "Data source has not been provided yet."
        return self.__from_clause

    def set_from_clause(
        self, from_clause: Union[Query, CompositeQuery, JoinClause[Entity]]
    ) -> None:
        self.__from_clause = from_clause
