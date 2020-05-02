import pytest
from typing import Any, MutableMapping, Set

from snuba.query.logical import Query as LogicalQuery
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_dsl.accessors import get_project_ids_in_query

test_cases = [
    (
        {"selected_columns": ["column1"], "conditions": [["project_id", "=", 100]]},
        {100},
    ),  # Simple single project condition
    (
        {
            "selected_columns": ["column1"],
            "conditions": [["project_id", "IN", [100, 200, 300]]],
        },
        {100, 200, 300},
    ),  # Multiple projects in the query
    (
        {
            "selected_columns": ["column1"],
            "conditions": [["project_id", "IN", (100, 200, 300)]],
        },
        {100, 200, 300},
    ),  # Multiple projects in the query provided as tuple
    (
        {"selected_columns": ["column1"], "conditions": []},
        set(),
    ),  # No project condition
    (
        {
            "selected_columns": ["column1"],
            "conditions": [
                ["project_id", "IN", [100, 200, 300]],
                ["project_id", "IN", [300, 400, 500]],
            ],
        },
        {300},
    ),  # Multiple project conditions, intersected together
    (
        {
            "selected_columns": ["column1"],
            "conditions": [
                [
                    ["project_id", "IN", [100, 200, 300]],
                    ["project_id", "IN", [300, 400, 500]],
                ]
            ],
        },
        {100, 200, 300, 400, 500},
    ),  # Multiple project conditions, in unnion
]


@pytest.mark.parametrize("query_body, expected_projects", test_cases)
def test_find_projects(
    query_body: MutableMapping[str, Any], expected_projects: Set[int]
) -> None:
    query = ClickhouseQuery(LogicalQuery(query_body, None))
    project_ids = get_project_ids_in_query(query, "project_id")

    assert project_ids == expected_projects
