from collections.abc import Callable, Iterable, Sequence

from snuba.query import LimitBy, OrderBy, SelectedExpression
from snuba.query import ProcessableQuery as AbstractQuery
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import ExpressionVisitor

# This defines the type of a Clickhouse query expression. It is used to make
# the interface of the expression translator more intuitive.
Expression = SnubaExpression


class Query(AbstractQuery[Table]):
    def __init__(
        self,
        from_clause: Table | None,
        # New data model to replace the one based on the dictionary
        selected_columns: Sequence[SelectedExpression] | None = None,
        array_join: Sequence[Expression] | None = None,
        condition: Expression | None = None,
        prewhere: Expression | None = None,
        groupby: Sequence[Expression] | None = None,
        having: Expression | None = None,
        order_by: Sequence[OrderBy] | None = None,
        limitby: LimitBy | None = None,
        limit: int | None = None,
        offset: int = 0,
        totals: bool = False,
        granularity: int | None = None,
        on_cluster: Expression | None = None,
        is_delete: bool = False,
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
            on_cluster=on_cluster,
            is_delete=is_delete,
        )

    def _get_expressions_impl(self) -> Iterable[Expression]:
        return self.__prewhere or []

    def _transform_expressions_impl(self, func: Callable[[Expression], Expression]) -> None:
        self.__prewhere = self.__prewhere.transform(func) if self.__prewhere else None

    def _transform_impl(self, visitor: ExpressionVisitor[Expression]) -> None:
        if self.__prewhere is not None:
            self.__prewhere = self.__prewhere.accept(visitor)

    def get_prewhere_ast(self) -> Expression | None:
        """
        Temporary method until pre where management is moved to Clickhouse query
        """
        return self.__prewhere

    def set_prewhere_ast_condition(self, condition: Expression | None) -> None:
        self.__prewhere = condition

    def _eq_functions(self) -> Sequence[str]:
        return tuple(super()._eq_functions()) + ("get_prewhere_ast",)
