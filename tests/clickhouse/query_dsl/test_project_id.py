from typing import Any, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.translator.query import identity_translate
from snuba.query.logical import Query
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query

test_cases: Sequence[Tuple[Mapping[str, Any], Optional[Set[int]]]] = [
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "=", 100],
            ],
        },
        {100},
    ),  # Simple single project condition
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", [100, 200, 300]],
            ],
        },
        {100, 200, 300},
    ),  # Multiple projects in the query
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", (100, 200, 300)],
            ],
        },
        {100, 200, 300},
    ),  # Multiple projects in the query provided as tuple
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
            ],
        },
        None,
    ),  # No project condition
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", [100, 200, 300]],
                ["project_id", "IN", [300, 400, 500]],
            ],
        },
        {300},
    ),  # Multiple project conditions, intersected together
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                [
                    ["project_id", "IN", [100, 200, 300]],
                    ["project_id", "IN", [300, 400, 500]],
                ],
            ],
        },
        None,
    ),  # Multiple project conditions, in union
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", [100, 200, 300]],
                ["project_id", "=", 400],
            ],
        },
        set(),
    ),  # A fairly stupid query
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["event_id", "=", "something"],
                [["ifNull", ["partition", 0]], "=", 1],
                ["project_id", "IN", [100, 200, 300]],
                [["count", ["offset"]], "=", 10],
                ["project_id", "=", 100],
            ],
        },
        {100},
    ),  # Multiple conditions in AND. Two project conditions
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", [100, 200, 300]],
                [["project_id", "=", 100], ["project_id", "=", 200]],
            ],
        },
        {100, 200},
    ),  # Main project list in a conditions and multiple project conditions in OR
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                ["project_id", "IN", [100, 200, 300]],
                [
                    [["ifNull", ["project_id", 1000]], "=", 100],
                    [["count", ["offset"]], "=", 10],
                    [["ifNull", ["project_id", 1000]], "=", 200],
                ],
            ],
        },
        {100, 200, 300},
    ),  # Main project list in a conditions and multiple project conditions within unsupported function calls
    (
        {
            "selected_columns": ["event_id"],
            "conditions": [
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
                [
                    [
                        "and",
                        [
                            ["equals", ["project_id", 100]],
                            ["equals", ["event_id", "'something'"]],
                        ],
                    ],
                    "=",
                    1,
                ],
                [
                    [
                        "and",
                        [
                            ["equals", ["project_id", 200]],
                            ["equals", ["platform", "'something_else'"]],
                        ],
                    ],
                    "=",
                    1,
                ],
            ],
        },
        set(),
    ),  # project_id in unsupported functions (cannot navigate into an "and" function)
    # TODO: make this work as it should through the AST.
]


@pytest.mark.redis_db
@pytest.mark.parametrize("query_body, expected_projects", test_cases)
def test_find_projects(
    query_body: MutableMapping[str, Any], expected_projects: Optional[Set[int]]
) -> None:
    events = get_dataset("events")
    if expected_projects is None:
        with pytest.raises(ParsingException):
            request = json_to_snql(query_body, "events")
            request.validate()
            query, _ = parse_snql_query(str(request.query), events)
            assert isinstance(query, Query)
            identity_translate(query)
    else:
        request = json_to_snql(query_body, "events")
        request.validate()
        query, _ = parse_snql_query(str(request.query), events)
        assert isinstance(query, Query)
        translated_query = identity_translate(query)
        project_ids_ast = get_object_ids_in_query_ast(translated_query, "project_id")
        assert project_ids_ast == expected_projects
