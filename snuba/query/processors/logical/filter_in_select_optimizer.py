import logging

from snuba import environment
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    DangerousRawSQL,
    ExpressionVisitor,
    FunctionCall,
    JsonPath,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api")

logger = logging.getLogger(__name__)


class FindConditionalAggregateFunctionsVisitor(
    ExpressionVisitor[list[FunctionCall | CurriedFunctionCall]]
):
    """
    Visitor that searches an expression for all conditional aggregate functions.
    Results are returned via get_matches function.

    Example:
    myexp = add(divide(sumIf(...), avgIf(...)), 100)
    visitor = FindConditionalAggregateFunctionsVisitor()
    myexp.accept(visitor)
    res = visitor.get_matches()

    Visitor implementation to find all conditional aggregate functions in an expression.
    Usage:
        >>> exp: Expression = add(divide(sumIf(...), avgIf(...)), 100)
        >>> visitor = FindConditionalAggregateFunctionsVisitor()
        >>> found = exp.accept(visitor)  # found = [sumIf(...), avgIf(...)]
    """

    def __init__(self) -> None:
        self._matches: list[FunctionCall | CurriedFunctionCall] = []

    def visit_literal(self, exp: Literal) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_column(self, exp: Column) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_subscriptable_reference(
        self, exp: SubscriptableReference
    ) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_function_call(self, exp: FunctionCall) -> list[FunctionCall | CurriedFunctionCall]:
        if exp.function_name[-2:] == "If":
            self._matches.append(exp)
        else:
            for param in exp.parameters:
                param.accept(self)
        return self._matches

    def visit_curried_function_call(
        self, exp: CurriedFunctionCall
    ) -> list[FunctionCall | CurriedFunctionCall]:
        if exp.internal_function.function_name[-2:] == "If":
            self._matches.append(exp)
        return self._matches

    def visit_argument(self, exp: Argument) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_lambda(self, exp: Lambda) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_dangerous_raw_sql(
        self, exp: DangerousRawSQL
    ) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches

    def visit_json_path(self, exp: JsonPath) -> list[FunctionCall | CurriedFunctionCall]:
        return self._matches


class FilterInSelectOptimizer(LogicalQueryProcessor):
    """
    This optimizer grabs all conditions from conditional aggregate functions in the select clause
    and adds them into the where clause. Example:

        SELECT sumIf(value, metric_id in (1,2,3,4) and status=200) / sumIf(value, metric_id in (1,2,3,4)),
               avgIf(value, metric_id in (3,4,5))
        FROM table

        becomes

        SELECT sumIf(value, metric_id in (1,2,3,4) and status=200) / sumIf(value, metric_id in (1,2,3,4)),
               avgIf(value, metric_id in (3,4,5))
        FROM table
        WHERE (metric_id in (1,2,3,4) and status=200) or metric_id in (1,2,3,4) or metric_id in (3,4,5)
    """

    def process_query(self, query: LogicalQuery, query_settings: QuerySettings) -> None:
        try:
            new_condition = self.get_select_filter(query)
        except Exception:
            logger.warning("Failed during optimization", exc_info=True, extra={"query": query})
            return
        if new_condition is not None:
            query.add_condition_to_ast(new_condition)
            metrics.increment("filter_in_select_optimizer_optimized")

    def get_select_filter(
        self,
        query: LogicalQuery | CompositeQuery[QueryEntity],
    ) -> FunctionCall | None:
        """
        Given a query, grabs all the conditions from conditional aggregates and lifts into
        one condition.

        ex: SELECT sumIf(value, metric_id in (1,2,3,4) and status=200),
                   avgIf(value, metric_id in (11,12) and status=400),
                   ...

        returns or((metric_id in (1,2,3,4) and status=200), (metric_id in (11,12) and status=400))
        """
        # find and grab all the conditional aggregate functions
        cond_agg_functions: list[FunctionCall | CurriedFunctionCall] = []
        for selected_exp in query.get_selected_columns():
            found = selected_exp.expression.accept(FindConditionalAggregateFunctionsVisitor())
            cond_agg_functions += found
        if len(cond_agg_functions) == 0:
            return None

        # validate the functions, and lift their conditions into new_condition, return it
        new_condition = None
        for func in cond_agg_functions:
            if len(func.parameters) != 2:
                raise ValueError(
                    f"expected conditional function to be of the form funcIf(val, condition), but was given one with {len(func.parameters)} parameters"
                )
            if not isinstance(func.parameters[1], FunctionCall):
                raise ValueError(
                    f"expected conditional function to be of the form funcIf(val, condition), but the condition is type {type(func.parameters[1])} rather than FunctionCall"
                )

            if new_condition is None:
                new_condition = func.parameters[1]
            else:
                new_condition = binary_condition("or", new_condition, func.parameters[1])
        return new_condition
