from functools import reduce
from typing import List, Optional, Sequence, Set

from snuba.clickhouse.query import Query
from snuba.query.types import Condition
from snuba.util import is_condition


def get_project_ids_in_query(query: Query, project_column: str) -> Optional[Set[int]]:
    """
    Finds the project ids this query is filtering according to the legacy query
    representation.

    It returns a set of referenced project_ids if relevant conditions are found or
    None if no project_id condition is found in the query. Empty set means that multiple
    conflicting project id conditions were found.

    This function looks into first level AND conditions and second level OR conditions
    but it cannot support project ids used as function parameters.
    Specific limitations:
    - If a project_id is a parameter of a function that returns the project_id itself
      it is not supported. It would be very hard to support every function without a
      whitelist/blacklist of allowed functions in Snuba queries.
    - boolean functions are not supported. So we do not unpack and/or/not conditions
      expressed as functions. We will be able to do that with the AST.
    - does not exclude projects referenced in NOT conditions.

    We are going to try to lift as many of these limitations as possible. So, please,
    do not rely on them for the correctness of your code.
    """

    def find_project_id_sets(conditions: Sequence[Condition],) -> Sequence[Set[int]]:
        """
        Scans a potentially nested sequence of conditions.
        For each simple condition adds to the output the set of project ids referenced
        by the condition.
        For each nested condition, it assumes it is a union of simple conditions
        (which is the only supported valid case by the Query object) and adds the union of the
        referenced project ids to the output.
        """
        project_id_sets: List[Set[int]] = list()
        for c in conditions:
            if is_condition(c):
                # This is a simple condition. Can extract the project ids directly.
                # Supports these kinds of conditions
                # ["col", "=", 1]
                # ["col", "IN", [1,2,3]]
                # ["col", "IN", (1,2,3)]
                if c[0] == project_column:
                    if c[1] == "=" and isinstance(c[2], int):
                        project_id_sets.append({c[2]})
                    elif c[1] == "IN" and all(
                        isinstance(project, int) for project in c[2]
                    ):
                        project_id_sets.append(set(c[2]))

            elif all(is_condition(second_level) for second_level in c):
                # This is supposed to be a union of simple conditions. Need to union
                # the sets of project ids.
                sets_to_unite = find_project_id_sets(c)
                if sets_to_unite:
                    project_id_sets.append(reduce(lambda x, y: x | y, sets_to_unite))
            else:
                raise ValueError(f"Invalid condition {conditions}")

        return project_id_sets

    all_project_id_sets = find_project_id_sets(query.get_conditions() or [])

    if not all_project_id_sets:
        return None
    return reduce(lambda x, y: x & y, all_project_id_sets)
