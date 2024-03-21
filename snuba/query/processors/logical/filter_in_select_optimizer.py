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


class FindConditionalAggregateFunctionsVisitor(ExpressionVisitor[None]):
    """
    Search an expression for all conditional aggregate functions.

    Might be able to use matchers.Pattern to extend this idea for general search but I was too lazy.
    """

    def __init__(self) -> None:
        self._matches: list[FunctionCall | CurriedFunctionCall] = []

    def get_matches(self) -> list[FunctionCall | CurriedFunctionCall]:
        return deepcopy(self._matches)

    def visit_literal(self, exp: Literal) -> None:
        return

    def visit_column(self, exp: Column) -> None:
        return

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> None:
        return

    def visit_function_call(self, exp: FunctionCall) -> None:
        if exp.function_name[-2:] == "If":
            self._matches.append(exp)
        else:
            for param in exp.parameters:
                param.accept(self)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> None:
        if exp.internal_function.function_name[-2:] == "If":
            self._matches.append(exp)

    def visit_argument(self, exp: Argument) -> None:
        return

    def visit_lambda(self, exp: Lambda) -> None:
        return


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
            except ValueError:
                raise

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
            finder = FindConditionalAggregateFunctionsVisitor()
            exp.accept(finder)
            found = finder.get_matches()
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
