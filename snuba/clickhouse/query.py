from typing import Callable, Iterable, Optional, Sequence

from snuba.query import LimitBy, OrderBy
from snuba.query import ProcessableQuery as AbstractQuery
from snuba.query import SelectionType
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import ExpressionVisitor

# This defines the type of a Clickhouse query expression. It is used to make
# the interface of the expression translator more intuitive.
Expression = SnubaExpression


class Query(AbstractQuery[Table]):
    def __init__(
        self,
        from_clause: Optional[Table],
        # New data model to replace the one based on the dictionary
        selected_columns: Optional[SelectionType] = None,
        array_join: Optional[Sequence[Expression]] = None,
        condition: Optional[Expression] = None,
        prewhere: Optional[Expression] = None,
        groupby: Optional[Sequence[Expression]] = None,
        having: Optional[Expression] = None,
        order_by: Optional[Sequence[OrderBy]] = None,
        limitby: Optional[LimitBy] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        totals: bool = False,
        granularity: Optional[int] = None,
    ) -> None:
        self.__prewhere = prewhere

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

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_prewhere_ast",)
