import copy
import math
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any as AnyType
from typing import List, Optional, Tuple, Union, cast

from snuba import environment, state, util
from snuba.clickhouse.query import Query
from snuba.datasets.plans.split_strategy import QuerySplitStrategy, SplitQueryRunner
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    combine_and_conditions,
    get_first_level_and_conditions,
    in_condition,
)
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.logical import SelectedExpression
from snuba.query.matchers import (
    Any,
    AnyExpression,
    Column,
    FunctionCall,
    Literal,
    Or,
    Param,
    String,
)
from snuba.request.request_settings import RequestSettings
from snuba.util import is_condition
from snuba.utils.metrics.backends.wrapper import MetricsWrapper
from snuba.web import QueryResult

metrics = MetricsWrapper(environment.metrics, "query.splitter")

# Every time we find zero results for a given step, expand the search window by
# this factor. Based on the assumption that the initial window is 2 hours, the
# worst case (there are 0 results in the database) would have us making 4
# queries before hitting the 90d limit (2+20+200+2000 hours == 92 days).
STEP_GROWTH = 10


def _identify_condition(condition: AnyType, field: str, operator: str) -> bool:
    return (
        is_condition(condition) and condition[0] == field and condition[1] == operator
    )


def _get_time_range(
    query: Query, timestamp_field: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Finds the minimal time range for this query. Which means, it finds
    the >= timestamp condition with the highest datetime literal and
    the < timestamp condition with the smallest and returns the interval
    in the form of a tuple of Literals. It only looks into first level
    AND conditions since, if the timestamp is nested in an OR we cannot
    say anything on how that compares to the other timestamp conditions.

    TODO: Consider making this part of the AST api if there are more use
    cases. It would require managing a few more corner cases for being part
    of the api.
    """

    condition_clause = query.get_condition_from_ast()
    if not condition_clause:
        return (None, None)

    max_lower_bound = None
    min_upper_bound = None
    for c in get_first_level_and_conditions(condition_clause):
        match = FunctionCall(
            None,
            Param(
                "operator",
                Or(
                    [
                        String(OPERATOR_TO_FUNCTION[">="]),
                        String(OPERATOR_TO_FUNCTION["<"]),
                    ]
                ),
            ),
            (
                Column(None, None, String(timestamp_field)),
                Literal(None, Param("timestamp", Any(datetime))),
            ),
        ).match(c)

        if match is not None:
            timestamp = cast(datetime, match.scalar("timestamp"))
            if match.string("operator") == OPERATOR_TO_FUNCTION[">="]:
                if not max_lower_bound or timestamp > max_lower_bound:
                    max_lower_bound = timestamp
            else:
                if not min_upper_bound or timestamp < min_upper_bound:
                    min_upper_bound = timestamp

    return (max_lower_bound, min_upper_bound)


def _replace_ast_condition(
    query: Query, field: str, operator: str, new_operand: Expression
) -> None:
    """
    Replaces a condition in the top level AND boolean condition
    in the query WHERE clause.
    """

    def replace_condition(expression: Expression) -> Expression:
        match = FunctionCall(
            None,
            String(OPERATOR_TO_FUNCTION[operator]),
            (Param("column", Column(None, None, String(field))), AnyExpression()),
        ).match(expression)

        return (
            expression
            if match is None
            else replace(
                expression, parameters=(match.expression("column"), new_operand)
            )
        )

    condition = query.get_condition_from_ast()
    if condition is not None:
        query.set_ast_condition(
            combine_and_conditions(
                [
                    replace_condition(c)
                    for c in get_first_level_and_conditions(condition)
                ]
            )
        )


def _replace_condition(
    query: Query, field: str, operator: str, new_literal: Union[str, List[AnyType]]
) -> None:
    query.set_conditions(
        [
            cond
            if not _identify_condition(cond, field, operator)
            else [field, operator, new_literal]
            for cond in query.get_conditions() or []
        ]
    )


class TimeSplitQueryStrategy(QuerySplitStrategy):
    """
    A strategy that breaks the time window into smaller ones and executes
    them in sequence.
    """

    def __init__(self, timestamp_col: str) -> None:
        self.__timestamp_col = timestamp_col

    def execute(
        self, query: Query, request_settings: RequestSettings, runner: SplitQueryRunner,
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
        if not orderby or orderby[0] != f"-{self.__timestamp_col}":
            return None

        conditions = query.get_conditions() or []
        from_date_str = next(
            (
                condition[2]
                for condition in conditions
                if _identify_condition(condition, self.__timestamp_col, ">=")
            ),
            None,
        )

        to_date_str = next(
            (
                condition[2]
                for condition in conditions
                if _identify_condition(condition, self.__timestamp_col, "<")
            ),
            None,
        )
        from_date_ast, to_date_ast = _get_time_range(query, self.__timestamp_col)

        if not from_date_str or not to_date_str:
            return None

        date_align, split_step = state.get_configs(
            [("date_align_seconds", 1), ("split_step", 3600)]  # default 1 hour
        )
        to_date = util.parse_datetime(to_date_str, date_align)
        from_date = util.parse_datetime(from_date_str, date_align)

        if from_date != from_date_ast:
            metrics.increment("mismatch.ast_from_date")
        if to_date != to_date_ast:
            metrics.increment("mismatch.ast_to_date")

        remaining_offset = query.get_offset()

        overall_result = None
        split_end = to_date
        split_start = max(split_end - timedelta(seconds=split_step), from_date)
        total_results = 0
        while split_start < split_end and total_results < limit:
            # We need to make a copy to use during the query execution because we replace
            # the start-end conditions on the query at each iteration of this loop.
            split_query = copy.deepcopy(query)

            _replace_condition(
                split_query, self.__timestamp_col, ">=", split_start.isoformat()
            )
            _replace_ast_condition(
                split_query, self.__timestamp_col, ">=", LiteralExpr(None, split_start)
            )
            _replace_condition(
                split_query, self.__timestamp_col, "<", split_end.isoformat()
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
            result = runner(split_query, request_settings)

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
                        split_end - timedelta(seconds=split_step), from_date
                    )
                except OverflowError:
                    split_start = from_date

        return overall_result


class ColumnSplitQueryStrategy(QuerySplitStrategy):
    """
    A strategy that performs column based splitting: if the client requests enough columns,
    a first query on the minimum set of columns is ran to load as little Clickhouse data
    as possible. A second query based on the results of the first is then executed to
    build the full result set.
    """

    def __init__(
        self, id_column: str, project_column: str, timestamp_column: str,
    ) -> None:
        self.__id_column = id_column
        self.__project_column = project_column
        self.__timestamp_column = timestamp_column

    def execute(
        self, query: Query, request_settings: RequestSettings, runner: SplitQueryRunner,
    ) -> Optional[QueryResult]:
        """
        Split query in 2 steps if a large number of columns is being selected.
            - First query only selects event_id, project_id and timestamp.
            - Second query selects all fields for only those events.
            - Shrink the date range.
        """
        if (
            not query.get_limit()
            or query.get_groupby()
            or query.get_aggregations()
            or not query.get_selected_columns()
        ):
            return None

        total_col_count = len(query.get_all_referenced_columns())
        total_ast_count = len(
            # We need to count the number of table/column name pairs
            # not the number of distinct Column objects in the query
            # so to avoid counting aliased columns multiple times.
            {
                (col.table_name, col.column_name)
                for col in query.get_all_ast_referenced_columns()
            }
        )
        if total_col_count != total_ast_count:
            metrics.increment("mismatch.total_referenced_columns")

        minimal_query = copy.deepcopy(query)
        minimal_query.set_selected_columns(
            [self.__id_column, self.__project_column, self.__timestamp_column]
        )
        # TODO: provide the table alias name to this splitter if we ever use it
        # in joins.
        minimal_query.set_ast_selected_columns(
            [
                SelectedExpression(
                    self.__id_column, ColumnExpr(None, None, self.__id_column)
                ),
                SelectedExpression(
                    self.__project_column, ColumnExpr(None, None, self.__project_column)
                ),
                SelectedExpression(
                    self.__timestamp_column,
                    ColumnExpr(None, None, self.__timestamp_column),
                ),
            ]
        )

        minimal_ast_count = len(
            {
                (col.table_name, col.column_name)
                for col in minimal_query.get_all_ast_referenced_columns()
            }
        )
        minimal_count = len(minimal_query.get_all_referenced_columns())
        if minimal_count != minimal_ast_count:
            metrics.increment("mismatch.minimaL_query_referenced_columns")

        if total_col_count <= minimal_count:
            return None

        # Ensure the minimal query is actually runnable on its own.
        if not minimal_query.validate_aliases():
            return None

        legacy_references = set(minimal_query.get_all_referenced_columns())
        ast_column_names = {
            c.column_name for c in minimal_query.get_all_ast_referenced_columns()
        }
        # Ensure the legacy minimal query as well (which does not expand aliases)
        # does contain alias references we removed when creating minimal_query.
        if legacy_references - ast_column_names:
            metrics.increment("columns.skip_invalid_legacy_query")
            return None

        result = runner(minimal_query, request_settings)
        del minimal_query

        if not result.result["data"]:
            return None

        # Making a copy just in case runner returned None (which would drive the execution
        # strategy to ignore the result of this splitter and try the next one).
        query = copy.deepcopy(query)

        event_ids = list(
            set([event[self.__id_column] for event in result.result["data"]])
        )
        query.add_conditions([(self.__id_column, "IN", event_ids)])
        query.add_condition_to_ast(
            in_condition(
                None,
                ColumnExpr(None, None, self.__id_column),
                [LiteralExpr(None, e_id) for e_id in event_ids],
            )
        )
        query.set_offset(0)
        # TODO: This is technically wrong. Event ids are unique per project, not globally.
        # So, if the minimal query only returned the same event_id from two projects, we
        # would be underestimating the limit here.
        query.set_limit(len(event_ids))

        project_ids = list(
            set([event[self.__project_column] for event in result.result["data"]])
        )
        _replace_condition(
            query, self.__project_column, "IN", project_ids,
        )
        _replace_ast_condition(
            query,
            self.__project_column,
            "IN",
            literals_tuple(None, [LiteralExpr(None, p_id) for p_id in project_ids]),
        )

        timestamps = [event[self.__timestamp_column] for event in result.result["data"]]
        _replace_condition(
            query,
            self.__timestamp_column,
            ">=",
            util.parse_datetime(min(timestamps)).isoformat(),
        )
        _replace_ast_condition(
            query,
            self.__timestamp_column,
            ">=",
            LiteralExpr(None, util.parse_datetime(min(timestamps))),
        )
        # We add 1 second since this gets translated to ('timestamp', '<', to_date)
        # and events are stored with a granularity of 1 second.
        _replace_condition(
            query,
            self.__timestamp_column,
            "<",
            (util.parse_datetime(max(timestamps)) + timedelta(seconds=1)).isoformat(),
        )
        _replace_ast_condition(
            query,
            self.__timestamp_column,
            "<",
            LiteralExpr(
                None, (util.parse_datetime(max(timestamps)) + timedelta(seconds=1)),
            ),
        )

        return runner(query, request_settings)
