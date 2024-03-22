import logging
from copy import deepcopy

from snuba import environment
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.state import get_int_config
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

    def visit_function_call(
        self, exp: FunctionCall
    ) -> list[FunctionCall | CurriedFunctionCall]:
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


class FilterInSelectOptimizer:
    """
    This optimizer lifts conditions in the select clause into the where clause,
    this is
    and adds the equivalent conditions to the where clause. Example:

        SELECT sumIf(value, metric_id in (1,2,3,4) and status=200)
        FROM table

        becomes

        SELECT sumIf(value, metric_id in (1,2,3,4))
        FROM table
        WHERE metric_id in (1,2,3,4) and status=200
    """

    def process_mql_query(
        self, query: LogicalQuery | CompositeQuery[QueryEntity]
    ) -> None:
        feat_flag = get_int_config("enable_filter_in_select_optimizer", default=1)
        if feat_flag:
            try:
                new_condition = self.get_select_filter(query)
                if new_condition is not None:
                    query.add_condition_to_ast(new_condition)
                    metrics.increment("kyles_optimizer_optimized")
            except Exception:
                logger.warning(
                    "Failed during optimization", exc_info=True, extra={"query": query}
                )
                return

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
            exp = selected_exp.expression
            found = exp.accept(FindConditionalAggregateFunctionsVisitor())
            if len(found) > 0:
                if len(cond_agg_functions) > 0:
                    raise ValueError(
                        "expected only one selected column to contain conditional aggregate functions but found multiple"
                    )
                else:
                    cond_agg_functions = found

        if len(cond_agg_functions) == 0:
            return None

        # validate the functions, and lift their conditions into new_condition, return it
        new_condition = None
        for func in cond_agg_functions:
            if len(func.parameters) != 2 or not isinstance(
                func.parameters[1], FunctionCall
            ):
                raise ValueError("unexpected form of function")

            if new_condition is None:
                new_condition = deepcopy(func.parameters[1])
            else:
                new_condition = binary_condition(
                    "or", deepcopy(func.parameters[1]), new_condition
                )
        return new_condition
