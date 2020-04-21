import copy
import math

from datetime import timedelta
from typing import Any, List, Optional, Union

from snuba import state, util
from snuba.datasets.plans.query_plan import QueryRunner
from snuba.datasets.plans.split_strategy import StorageQuerySplitStrategy
from snuba.query.query import Query
from snuba.request import Request
from snuba.util import is_condition
from snuba.web import RawQueryResult

# Every time we find zero results for a given step, expand the search window by
# this factor. Based on the assumption that the initial window is 2 hours, the
# worst case (there are 0 results in the database) would have us making 4
# queries before hitting the 90d limit (2+20+200+2000 hours == 92 days).
STEP_GROWTH = 10


def _identify_condition(condition: Any, field: str, operand: str) -> bool:
    return is_condition(condition) and condition[0] == field and condition[1] == operand


def _replace_condition(
    query: Query, field: str, operand: str, new_literal: Union[str, List[Any]]
) -> None:
    query.set_conditions(
        [
            cond
            if not _identify_condition(cond, field, operand)
            else [field, operand, new_literal]
            for cond in query.get_conditions() or []
        ]
    )


class TimeSplitQueryStrategy(StorageQuerySplitStrategy):
    """
    A strategy that breaks the time window into smaller ones and executes
    them in sequence.
    """

    def __init__(self, timestamp_col: str) -> None:
        self.__timestamp_col = timestamp_col

    def execute(
        self, request: Request, runner: QueryRunner
    ) -> Optional[RawQueryResult]:
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
        limit = request.query.get_limit()
        if limit is None or request.query.get_groupby():
            return None

        if request.query.get_offset() >= 1000:
            return None

        orderby = request.query.get_orderby()
        if not orderby or orderby[0] != f"-{self.__timestamp_col}":
            return None

        conditions = request.query.get_conditions() or []
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

        if not from_date_str or not to_date_str:
            return None

        date_align, split_step = state.get_configs(
            [("date_align_seconds", 1), ("split_step", 3600)]  # default 1 hour
        )
        to_date = util.parse_datetime(to_date_str, date_align)
        from_date = util.parse_datetime(from_date_str, date_align)

        remaining_offset = request.query.get_offset()

        overall_result = None
        split_end = to_date
        split_start = max(split_end - timedelta(seconds=split_step), from_date)
        total_results = 0
        while split_start < split_end and total_results < limit:
            # We need to make a copy to use during the query execution because we replace
            # the start-end conditions on the query at each iteration of this loop.
            split_request = copy.deepcopy(request)

            _replace_condition(
                split_request.query, self.__timestamp_col, ">=", split_start.isoformat()
            )
            _replace_condition(
                split_request.query, self.__timestamp_col, "<", split_end.isoformat()
            )
            # Because its paged, we have to ask for (limit+offset) results
            # and set offset=0 so we can then trim them ourselves.
            split_request.query.set_offset(0)
            split_request.query.set_limit(limit - total_results + remaining_offset)

            # At every iteration we only append the "data" key from the results returned by
            # the runner. The "extra" key is only populated at the first iteration of the
            # loop and never changed.
            result = runner(split_request)

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


class ColumnSplitQueryStrategy(StorageQuerySplitStrategy):
    """
    A strategy that performs column based splitting: if the client requests enough columns,
    a first query on the minimum set of columns is ran to load as little Clickhouse data
    as possible. A second query based on the results of the first is then executed to
    build the full result set.
    """

    def __init__(
        self, id_column: str, project_column: str, timestamp_column: str
    ) -> None:
        self.__id_column = id_column
        self.__project_column = project_column
        self.__timestamp_column = timestamp_column

    def execute(
        self, request: Request, runner: QueryRunner
    ) -> Optional[RawQueryResult]:
        """
        Split query in 2 steps if a large number of columns is being selected.
            - First query only selects event_id, project_id and timestamp.
            - Second query selects all fields for only those events.
            - Shrink the date range.
        """
        if (
            not request.query.get_limit()
            or request.query.get_groupby()
            or request.query.get_aggregations()
            or not request.query.get_selected_columns()
        ):
            return None

        total_col_count = len(request.query.get_all_referenced_columns())
        minimal_request = copy.deepcopy(request)
        minimal_request.query.set_selected_columns(
            [self.__id_column, self.__project_column, self.__timestamp_column]
        )

        if total_col_count <= len(minimal_request.query.get_all_referenced_columns()):
            return None

        result = runner(minimal_request)
        del minimal_request

        if result.result["data"]:
            request = copy.deepcopy(request)

            event_ids = list(
                set([event[self.__id_column] for event in result.result["data"]])
            )
            request.query.add_conditions([(self.__id_column, "IN", event_ids)])
            request.query.set_offset(0)
            # TODO: This is technically wrong. Event ids are unique per project, not globally.
            # So, if the minimal query only returned the same event_id from two projects, we
            # would be underestimating the limit here.
            request.query.set_limit(len(event_ids))

            project_ids = list(
                set([event[self.__project_column] for event in result.result["data"]])
            )
            _replace_condition(
                request.query, self.__project_column, "IN", project_ids,
            )

            timestamps = [
                event[self.__timestamp_column] for event in result.result["data"]
            ]
            _replace_condition(
                request.query,
                self.__timestamp_column,
                ">=",
                util.parse_datetime(min(timestamps)).isoformat(),
            )
            # We add 1 second since this gets translated to ('timestamp', '<', to_date)
            # and events are stored with a granularity of 1 second.
            _replace_condition(
                request.query,
                self.__timestamp_column,
                "<",
                (
                    util.parse_datetime(max(timestamps)) + timedelta(seconds=1)
                ).isoformat(),
            )

        return runner(request)
