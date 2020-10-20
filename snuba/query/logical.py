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
from snuba.datasets.schemas import RelationalSource
from snuba.query import Limitby, OrderBy
from snuba.query import Query as AbstractQuery
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Expression, ExpressionVisitor

Aggregation = Union[
    Tuple[Any, Any, Any], Sequence[Any],
]


class Query(AbstractQuery[RelationalSource]):
    """
    Represents the logical query during query processing.
    This means the query class used between parsing and query translation.

    TODO: This query is supposed to rely on entities as data source.
    This will happen in a following step.
    """

    def __init__(
        self,
        body: MutableMapping[str, Any],  # Temporary
        data_source: Optional[RelationalSource],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[Sequence[SelectedExpression]] = None,
        array_join: Optional[Expression] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        entity: Optional[Entity] = None,
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
        # Temporary, these will only be in the logical query and not
        # in the physical one.
        self.__prewhere = prewhere
        self.__entity = entity

        super().__init__(
            from_clause=data_source,
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

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return self.__prewhere or []

    def _transform_expressions_impl(
        self, func: Callable[[Expression], Expression]
    ) -> None:
        self.__prewhere = self.__prewhere.transform(func) if self.__prewhere else None

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        if self.__prewhere is not None:
            self.__prewhere = self.__prewhere.accept(visitor)

    def get_prewhere_ast(self) -> Optional[Expression]:
        """
        Temporary method until pre where management is moved to Clickhouse query
        """
        return self.__prewhere

    def set_prewhere_ast_condition(self, condition: Optional[Expression]) -> None:
        self.__prewhere = condition

    def get_final(self) -> bool:
        return self.__final

    def set_final(self, final: bool) -> None:
        self.__final = final

    @deprecated(
        details="Do not access the internal query representation "
        "use the specific accessor methods instead."
    )
    def get_body(self) -> Mapping[str, Any]:
        return self.__body

    def set_entity(self, entity: Entity) -> None:
        assert self.__entity is None
        self.__entity = entity

    def get_entity(self) -> Entity:
        assert self.__entity is not None
        return self.__entity
