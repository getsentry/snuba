from functools import reduce
from typing import Any, Sequence, List

from snuba.clickhouse.query import Query
from snuba.query.types import Condition
from snuba.util import is_condition, is_function


def _is_project_column_in_function(function: Any, project_column: str) -> bool:
    verified_function = is_function(function)
    if not verified_function:
        return False

    if not verified_function[0] in ("assumeNotNull", "ifNull"):
        return False

    if len(verified_function[1]) < 1:
        return False

    return (
        isinstance(verified_function[1][0], str)
        and project_column == verified_function[1][0]
    )


def _find_projects_in_condition(condition: Condition, project_column: str) -> Set[int]:
    """
    Looks for the project ids referenced in a simple condition. The condition must
    be a valid one. This method does not check for that.
    It supports these formats:
    ["col", "=", 1]
    ["col", "IN", [1,2,3]]
    [["f", ["col"]], "=", 1] if f is one of assumeNotNull, ifNull
    """

    if not project_column == condition[0] and not _is_project_column_in_function(
        condition[0], project_column
    ):
        return set()

    if condition[1] == "=" and isinstance(condition[2], int):
        return {condition[2]}

    if condition[1] == "IN" and all(isinstance(c, int) for c in condition[2]):
        return set(condition[2])

    return set()


def get_project_ids_in_query(query: Query, project_column: str) -> Set[int]:
    project_id_set: List[Set[int]] = list()
    for first_level in query.get_conditions() or []:
        if all(is_condition(second_level) for second_level in first_level):
            # This is a OR nested into an AND
            condition_project_ids = reduce(
                lambda x, y: x | y,
                (_find_projects_in_condition(s, project_column) for s in first_level),
            )
            if condition_project_ids:
                project_id_set.append(condition_project_ids)
        else:
            if is_condition(first_level):
                condition_project_ids = _find_projects_in_condition(
                    first_level, project_column
                )
                if condition_project_ids:
                    project_id_set.append(condition_project_ids)
    if not project_id_set:
        return set()
    return reduce(lambda x, y: x & y, project_id_set)
