from typing import Optional, Set

from snuba import state
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.query.logical import Query as LogicalQuery


def is_in_experiment(
    query: LogicalQuery,
    referrer: str,
    project_key: str,
    allowed_referrers: Optional[Set[str]],
) -> bool:
    if allowed_referrers is not None and referrer not in allowed_referrers:
        return False

    project_ids = get_object_ids_in_query_ast(query, "project_id")
    if not project_ids:
        return False

    test_projects_raw = state.get_config(project_key, "")
    test_projects = set()
    if (
        isinstance(test_projects_raw, str) and test_projects_raw != ""
    ):  # should be in the form [1,2,3]
        test_projects_raw = test_projects_raw[1:-1]
        test_projects = set(int(p) for p in test_projects_raw.split(",") if p)
    elif isinstance(test_projects_raw, (int, float)):
        test_projects = {int(test_projects_raw)}

    return project_ids.issubset(test_projects)
