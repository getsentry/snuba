import copy
import logging
import math
from dataclasses import replace
from datetime import timedelta
from typing import Optional

from snuba import environment, settings, state, util
from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.plans.splitters import QuerySplitStrategy, SplitQueryRunner
from snuba.query import OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_and_conditions,
    in_condition,
)
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import CurriedFunctionCall as CurriedFunctionCallExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import AnyExpression, Column, FunctionCall, Or, Param, String
from snuba.query.query_settings import QuerySettings
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.web import QueryResult

logger = logging.getLogger("snuba.query.split")
metrics = MetricsWrapper(environment.metrics, "query.splitter")

# Every time we find zero results for a given step, expand the search window by
# this factor. Based on the assumption that the initial window is 2 hours, the
# worst case (there are 0 results in the database) would have us making 4
# queries before hitting the 90d limit (2+20+200+2000 hours == 92 days).
STEP_GROWTH = 10


def _replace_ast_condition(
    query: Query, field: str, operator: str, new_operand: Expression
) -> None:
    """
    Replaces a condition in the top level AND boolean condition
    in the query WHERE clause.
    """

    def replace_condition(expression: Expression) -> Expression:
        match = FunctionCall(
            String(OPERATOR_TO_FUNCTION[operator]),
            (Param("column", Column(None, String(field))), AnyExpression()),
        ).match(expression)

        return (
            expression
            if match is None
            else replace(
                expression, parameters=(match.expression("column"), new_operand)
            )
        )

    condition = query.get_condition()
    if condition is not None:
        query.set_ast_condition(
            combine_and_conditions(
                [
                    replace_condition(c)
                    for c in get_first_level_and_conditions(condition)
                ]
            )
        )


class TimeSplitQueryStrategy(QuerySplitStrategy):
    """
    A strategy that breaks the time window into smaller ones and executes
    them in sequence.
    """

    def __init__(self, timestamp_col: str) -> None:
        self.__timestamp_col = timestamp_col

    def execute(
        self,
        query: Query,
        query_settings: QuerySettings,
        runner: SplitQueryRunner,
    ) -> Optional[QueryResult]:
        """
        If a query is:
            - ORDER BY timestamp DESC
            - has no grouping
            - has an offset/limit
            - has a large time range
        We know we have to reverse-sort the entire set of rows to return the small
        chunk at the end of the time range, so optimistically split the time range
        into smaller increments, and start with the last one, so that we can potentially
        avoid querying the entire range.
        """
        limit = query.get_limit()
        if limit is None or query.get_groupby():
            return None

        if query.get_offset() >= 1000:
            return None

        orderby = query.get_orderby()
        if (
            not orderby
            or orderby[0].direction != OrderByDirection.DESC
            or not isinstance(orderby[0].expression, ColumnExpr)
            or not orderby[0].expression.column_name == self.__timestamp_col
        ):
            return None

        from_date_ast, to_date_ast = get_time_range(query, self.__timestamp_col)

        if from_date_ast is None or to_date_ast is None:
            return None

        date_align, split_step = state.get_configs(
            [("date_align_seconds", 1), ("split_step", 3600)]  # default 1 hour
        )
        assert isinstance(split_step, int)
        remaining_offset = query.get_offset()

        overall_result: Optional[QueryResult] = None
        split_end = to_date_ast
        split_start = max(split_end - timedelta(seconds=split_step), from_date_ast)
        total_results = 0
        while split_start < split_end and total_results < limit:
            # We need to make a copy to use during the query execution because we replace
            # the start-end conditions on the query at each iteration of this loop.
            split_query = copy.deepcopy(query)

            _replace_ast_condition(
                split_query, self.__timestamp_col, ">=", LiteralExpr(None, split_start)
            )
            _replace_ast_condition(
                split_query, self.__timestamp_col, "<", LiteralExpr(None, split_end)
            )

            # Because its paged, we have to ask for (limit+offset) results
            # and set offset=0 so we can then trim them ourselves.
            split_query.set_offset(0)
            split_query.set_limit(limit - total_results + remaining_offset)

            # At every iteration we only append the "data" key from the results returned by
            # the runner. The "extra" key is only populated at the first iteration of the
            # loop and never changed.
            result = runner(split_query, query_settings)

            if overall_result is None:
                overall_result = result
            else:
                overall_result.result["data"].extend(result.result["data"])

            if remaining_offset > 0 and len(overall_result.result["data"]) > 0:
                to_trim = min(remaining_offset, len(overall_result.result["data"]))
                overall_result.result["data"] = overall_result.result["data"][to_trim:]
                remaining_offset -= to_trim

            total_results = len(overall_result.result["data"])

            if total_results < limit:
                if len(result.result["data"]) == 0:
                    # If we got nothing from the last query, expand the range by a static factor
                    split_step = split_step * STEP_GROWTH
                else:
                    # If we got some results but not all of them, estimate how big the time
                    # range should be for the next query based on how many results we got for
                    # our last query and its time range, and how many we have left to fetch.
                    remaining = limit - total_results
                    split_step = split_step * math.ceil(
                        remaining / float(len(result.result["data"]))
                    )

                # Set the start and end of the next query based on the new range.
                split_end = split_start
                try:
                    split_start = max(
                        split_end - timedelta(seconds=split_step), from_date_ast
                    )
                except OverflowError:
                    split_start = from_date_ast

        return overall_result


class ColumnSplitQueryStrategy(QuerySplitStrategy):
    """
    A strategy that performs column based splitting: if the client requests enough columns,
    a first query on the minimum set of columns is ran to load as little Clickhouse data
    as possible. A second query based on the results of the first is then executed to
    build the full result set.
    """

    def __init__(
        self,
        id_column: str,
        project_column: str,
        timestamp_column: str,
    ) -> None:
        self.__id_column = id_column
        self.__project_column = project_column
        self.__timestamp_column = timestamp_column

    def execute(
        self,
        query: Query,
        query_settings: QuerySettings,
        runner: SplitQueryRunner,
    ) -> Optional[QueryResult]:
        """
        Split query in 2 steps if a large number of columns is being selected.
            - First query only selects event_id, project_id and timestamp.
            - Second query selects all fields for only those events.
            - Shrink the date range.
        """
        limit = query.get_limit()
        if (
            limit is None
            or limit == 0
            or query.get_groupby()
            or not query.get_selected_columns()
        ):
            return None

        if limit > settings.COLUMN_SPLIT_MAX_LIMIT:
            metrics.increment("column_splitter.query_above_limit")
            return None

        # Do not split if there is already a = or IN condition on an ID column
        id_column_matcher = FunctionCall(
            Or([String(ConditionFunctions.EQ), String(ConditionFunctions.IN)]),
            (
                Column(None, String(self.__id_column)),
                AnyExpression(),
            ),
        )

        for expr in query.get_condition() or []:
            match = id_column_matcher.match(expr)

            if match:
                return None

        # We need to count the number of table/column name pairs
        # not the number of distinct Column objects in the query
        # so to avoid counting aliased columns multiple times.
        selected_columns = {
            (col.table_name, col.column_name)
            for col in query.get_columns_referenced_in_select()
        }

        if len(selected_columns) < settings.COLUMN_SPLIT_MIN_COLS:
            metrics.increment("column_splitter.main_query_min_threshold")
            return None

        minimal_query = copy.deepcopy(query)

        # TODO: provide the table alias name to this splitter if we ever use it
        # in joins.
        minimal_query.set_ast_selected_columns(
            [
                SelectedExpression(
                    self.__id_column,
                    ColumnExpr(self.__id_column, None, self.__id_column),
                ),
                SelectedExpression(
                    self.__project_column,
                    ColumnExpr(self.__project_column, None, self.__project_column),
                ),
                SelectedExpression(
                    self.__timestamp_column,
                    ColumnExpr(self.__timestamp_column, None, self.__timestamp_column),
                ),
            ]
        )

        for exp in minimal_query.get_all_expressions():
            if exp.alias in (
                self.__id_column,
                self.__project_column,
                self.__timestamp_column,
            ) and not (isinstance(exp, ColumnExpr) and exp.column_name == exp.alias):
                logger.warning(
                    "Potential alias shadowing due to column splitter",
                    extra={"expression": exp},
                    exc_info=True,
                )

        # Ensures the AST minimal query is actually runnable on its own.
        if not minimal_query.validate_aliases():
            return None

        # There is a Clickhouse bug where if functions in the ORDER BY clause are not in the SELECT,
        # they fail on distributed tables. For that specific case, skip the query splitter.
        for orderby in minimal_query.get_orderby():
            if isinstance(
                orderby.expression, (FunctionCallExpr, CurriedFunctionCallExpr)
            ):
                metrics.increment("column_splitter.orderby_has_a_function")
                return None

        result = runner(minimal_query, query_settings)
        del minimal_query

        if not result.result["data"]:
            metrics.increment("column_splitter.no_data_from_minimal_query")
            return None

        # Making a copy just in case runner returned None (which would drive the execution
        # strategy to ignore the result of this splitter and try the next one).
        query = copy.deepcopy(query)

        event_ids = list(
            set([event[self.__id_column] for event in result.result["data"]])
        )
        if len(event_ids) > settings.COLUMN_SPLIT_MAX_RESULTS:
            # We may be runing a query that is beyond clickhouse maximum query size,
            # so we cowardly abandon.
            metrics.increment("column_splitter.intermediate_results_beyond_limit")
            return None

        query.add_condition_to_ast(
            in_condition(
                ColumnExpr(None, None, self.__id_column),
                [LiteralExpr(None, e_id) for e_id in event_ids],
            )
        )
        query.set_offset(0)
        query.set_limit(len(result.result["data"]))

        project_ids = list(
            set([event[self.__project_column] for event in result.result["data"]])
        )
        _replace_ast_condition(
            query,
            self.__project_column,
            "IN",
            literals_tuple(None, [LiteralExpr(None, p_id) for p_id in project_ids]),
        )

        timestamps = [event[self.__timestamp_column] for event in result.result["data"]]
        _replace_ast_condition(
            query,
            self.__timestamp_column,
            ">=",
            LiteralExpr(None, util.parse_datetime(min(timestamps))),
        )
        # We add 1 second since this gets translated to ('timestamp', '<', to_date)
        # and events are stored with a granularity of 1 second.
        _replace_ast_condition(
            query,
            self.__timestamp_column,
            "<",
            LiteralExpr(
                None,
                (util.parse_datetime(max(timestamps)) + timedelta(seconds=1)),
            ),
        )
        return runner(query, query_settings)
