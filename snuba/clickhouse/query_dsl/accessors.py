from functools import reduce
from typing import Optional, Sequence, Set, List

from snuba.clickhouse.query import Query
from snuba.query.types import Condition
from snuba.util import is_condition


def _find_projects_in_condition(
    condition: Condition, project_column: str
) -> Optional[Set[int]]:
    """
    Looks for the project ids referenced in a simple condition. The condition must
    be a valid one. This method does not check for that.
    It supports these formats:
    ["col", "=", 1]
    ["col", "IN", [1,2,3]]

    This function does not support project columns referenced as function arguments
    like assertNotNull(project_id) == x
    TODO: extend this to support projects when they are function arguments with the AST
    """

    if not project_column == condition[0]:
        return None

    if condition[1] == "=" and isinstance(condition[2], int):
        return {condition[2]}

    if condition[1] == "IN" and all(isinstance(c, int) for c in condition[2]):
        return set(condition[2])

    return None


def get_project_ids_in_query(query: Query, project_column: str) -> Optional[Set[int]]:
    """
    Finds the project ids this query is filtering onto according to the legacy query
    representation.

    This function looks into first level AND conditions, second level OR conditions but
    it cannot support project ids used as function parameters.
    Specific limitations:
    - If a project_id is a parameter of a function that returns the project_id itself
      this is not supported. It would be very hard to support every function without a
      whitelist/blacklist of allowed functions in Snuba queries.
    - boolean functions are not supported. So we do not unpack and/or/not conditions
      expressed as functions. We will be able to do that with the AST.
    - does not exclude projects referenced in NOT conditions.

    We are going to try to lift as many of these limitations as possible. So, please,
    do not rely on them for the correctness of your code.
    """

    def find_project_id_conditions(
        conditions: Sequence[Condition],
    ) -> Sequence[Set[int]]:
        """
        Scans a potentially nested sequence of conditions.
        For each simple condition adds to the output the set of project ids referenced
        by the condition.
        For each nested condition, it assumes it is a union of simple conditions
        (which is the only type supported by Snuba language) and adds the union of the
        referenced project ids to the output.
        """
        project_id_sets: List[Set[int]] = list()
        for c in conditions:
            if is_condition(c):
                # This is a simple condition. Can extract the project ids directly.
                condition_project_ids = _find_projects_in_condition(c, project_column)
                if condition_project_ids:
                    project_id_sets.append(condition_project_ids)
            elif all(is_condition(second_level) for second_level in c):
                # This is supposed to be a union of simple conditions. Need to union
                # the sets of project ids.
                sets_to_unite = find_project_id_conditions(c)
                if sets_to_unite:
                    project_id_sets.append(reduce(lambda x, y: x | y, sets_to_unite))
            else:
                raise ValueError(f"Invalid condition {conditions}")

        return project_id_sets

    all_project_id_sets = find_project_id_conditions(query.get_conditions() or [])

    if not all_project_id_sets:
        return None
    return reduce(lambda x, y: x & y, all_project_id_sets)
