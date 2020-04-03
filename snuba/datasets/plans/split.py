import copy
from datetime import timedelta
from typing import Any, List, NamedTuple, Sequence, Union
import math

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


def _is_query_splittable(query: Query) -> bool:
    (use_split,) = state.get_configs([("use_split", 0)])
    query_limit = query.get_limit()
    limit = query_limit if query_limit is not None else 0

    return use_split and limit and not query.get_groupby()


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
    def __init__(self, timestamp_col: str) -> None:
        self.__timestamp_col = timestamp_col

    def can_execute(self, request: Request) -> bool:
        remaining_offset = request.query.get_offset()
        orderby = util.to_list(request.query.get_orderby())

        has_from_date = any(
            _identify_condition(condition, self.__timestamp_col, ">=")
            for condition in request.query.get_conditions() or []
        )

        has_to_date = any(
            _identify_condition(condition, self.__timestamp_col, "<")
            for condition in request.query.get_conditions() or []
        )

        return (
            _is_query_splittable(request.query)
            and has_to_date
            and has_from_date
            and orderby[:1] == [f"-{self.__timestamp_col}"]
            and remaining_offset < 1000
        )

    def execute(self, request: Request, runner: QueryRunner) -> RawQueryResult:
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
        date_align, split_step = state.get_configs(
            [("date_align_seconds", 1), ("split_step", 3600)]  # default 1 hour
        )

        query_limit = request.query.get_limit()
        limit = query_limit if query_limit is not None else 0
        remaining_offset = request.query.get_offset()

        to_date_str = next(
            condition[2]
            for condition in request.query.get_conditions() or []
            if _identify_condition(condition, self.__timestamp_col, "<")
        )
        from_date_str = next(
            condition[2]
            for condition in request.query.get_conditions() or []
            if _identify_condition(condition, self.__timestamp_col, ">=")
        )

        to_date = util.parse_datetime(to_date_str, date_align)
        from_date = util.parse_datetime(from_date_str, date_align)

        overall_result = None
        split_end = to_date
        split_start = max(split_end - timedelta(seconds=split_step), from_date)
        total_results = 0
        while split_start < split_end and total_results < limit:
            # The query function may mutate the request body during query
            # evaluation, so we need to copy the body to ensure that the query
            # has not been modified in between this call and the next loop
            # iteration, if needed.
            # XXX: The extra data is carried across from the initial response
            # and never updated.
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


class ColumnSplitSpec(NamedTuple):
    """
    Provides the column names needed to perform column splitting.
    id_column represent the identity of the row.

    """

    id_column: str
    project_column: str
    timestamp_column: str

    def get_min_columns(self) -> Sequence[str]:
        return [self.id_column, self.project_column, self.timestamp_column]


class ColumnSplitQueryStrategy(StorageQuerySplitStrategy):
    def __init__(self, spec: ColumnSplitSpec) -> None:
        self.__column_split_spec = spec

    def can_execute(self, request: Request) -> bool:
        if not _is_query_splittable(request.query):
            return False

        total_col_count = len(request.query.get_all_referenced_columns())
        copied_query = copy.deepcopy(request.query)
        copied_query.set_selected_columns(self.__column_split_spec.get_min_columns())
        min_col_count = len(copied_query.get_all_referenced_columns())

        return (
            self.__column_split_spec
            and request.query.get_selected_columns()
            and not request.query.get_aggregations()
            and total_col_count > min_col_count
        )

    def execute(self, request: Request, runner: QueryRunner) -> RawQueryResult:
        """
        Split query in 2 steps if a large number of columns is being selected.
            - First query only selects event_id and project_id.
            - Second query selects all fields for only those events.
            - Shrink the date range.
        """
        # The query function may mutate the request body during query
        # evaluation, so we need to copy the body to ensure that the query has
        # not been modified by the time we're ready to run the full query.
        minimal_request = copy.deepcopy(request)
        minimal_request.query.set_selected_columns(
            self.__column_split_spec.get_min_columns()
        )
        result = runner(minimal_request)
        del minimal_request

        if result.result["data"]:
            request = copy.deepcopy(request)

            event_ids = list(
                set(
                    [
                        event[self.__column_split_spec.id_column]
                        for event in result.result["data"]
                    ]
                )
            )
            request.query.add_conditions(
                [(self.__column_split_spec.id_column, "IN", event_ids)]
            )
            request.query.set_offset(0)
            request.query.set_limit(len(event_ids))

            project_ids = list(
                set(
                    [
                        event[self.__column_split_spec.project_column]
                        for event in result.result["data"]
                    ]
                )
            )
            _replace_condition(
                request.query,
                self.__column_split_spec.project_column,
                "IN",
                project_ids,
            )

            timestamp_field = self.__column_split_spec.timestamp_column
            timestamps = [event[timestamp_field] for event in result.result["data"]]
            _replace_condition(
                request.query,
                timestamp_field,
                ">=",
                util.parse_datetime(min(timestamps)).isoformat(),
            )
            # We add 1 second since this gets translated to ('timestamp', '<', to_date)
            # and events are stored with a granularity of 1 second.
            _replace_condition(
                request.query,
                timestamp_field,
                "<",
                (
                    util.parse_datetime(max(timestamps)) + timedelta(seconds=1)
                ).isoformat(),
            )

        return runner(request)
