from typing import Any, MutableMapping

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.datasets.entities import EntityKey, EntityKeys
from snuba.datasets.factory import get_dataset
from snuba.query.snql.parser import parse_snql_query

test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, EntityKeys.DISCOVER_TRANSACTIONS),
    (
        {"conditions": [["type", "=", "transaction"], ["duration", ">", 1000]]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    ({"conditions": [["type", "=", "error"]]}, EntityKeys.DISCOVER_EVENTS),
    (
        {"conditions": [[["type", "=", "error"], ["type", "=", "transaction"]]]},
        EntityKeys.DISCOVER,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "or",
                        [
                            ["equals", ["type", "transaction"]],
                            ["equals", ["type", "default"]],
                        ],
                    ],
                    "=",
                    1,
                ]
            ]
        },
        EntityKeys.DISCOVER,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "and",
                        [
                            ["equals", ["duration", 10]],
                            ["notEquals", ["type", "error"]],
                        ],
                    ],
                    "=",
                    1,
                ]
            ]
        },
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "and",
                        [
                            ["notEquals", ["type", "transaction"]],
                            ["notEquals", ["type", "error"]],
                        ],
                    ],
                    "=",
                    1,
                ]
            ]
        },
        EntityKeys.DISCOVER_EVENTS,
    ),
    ({"conditions": [["type", "!=", "transaction"]]}, EntityKeys.DISCOVER_EVENTS),
    ({"conditions": []}, EntityKeys.DISCOVER),
    ({"conditions": [["duration", "=", 0]]}, EntityKeys.DISCOVER_TRANSACTIONS),
    (
        {"conditions": [["event_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    (
        {"conditions": [["group_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        EntityKeys.DISCOVER,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "and",
                        [
                            ["notEquals", ["type", "transaction"]],
                            ["notEquals", ["type", "error"]],
                        ],
                    ],
                    "=",
                    1,
                ],
                ["duration", "=", 0],
            ]
        },
        EntityKeys.DISCOVER_EVENTS,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "and",
                        [
                            ["notEquals", ["type", "default"]],
                            ["notEquals", ["type", "error"]],
                        ],
                    ],
                    "=",
                    1,
                ],
                ["duration", "=", 0],
            ]
        },
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    # # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, EntityKeys.DISCOVER_EVENTS),
    ({"selected_columns": ["span_id"]}, EntityKeys.DISCOVER),
    ({"selected_columns": ["group_id", "span_id"]}, EntityKeys.DISCOVER_EVENTS),
    (
        {"aggregations": [["max", "duration", "max_duration"]]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    (
        {"aggregations": [["apdex(duration, 300)", None, "apdex_duration_300"]]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    (
        {"aggregations": [["failure_rate()", None, "failure_rate"]]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    ({"aggregations": [["isHandled()", None, "handled"]]}, EntityKeys.DISCOVER_EVENTS),
    ({"aggregations": [["notHandled()", None, "handled"]]}, EntityKeys.DISCOVER_EVENTS),
    (
        {"selected_columns": ["measurements[lcp.elementSize]"]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
    (
        {"selected_columns": ["span_op_breakdowns[ops.http]"]},
        EntityKeys.DISCOVER_TRANSACTIONS,
    ),
]


@pytest.mark.parametrize("query_body, expected_entity", test_data)
def test_data_source(
    query_body: MutableMapping[str, Any],
    expected_entity: EntityKey,
) -> None:
    dataset = get_dataset("discover")
    # HACK until these are converted to proper SnQL queries
    if not query_body.get("conditions"):
        query_body["conditions"] = []
    query_body["conditions"] += [
        ["timestamp", ">=", "2020-01-01T12:00:00"],
        ["timestamp", "<", "2020-01-02T12:00:00"],
        ["project_id", "=", 1],
    ]
    if not query_body.get("selected_columns"):
        query_body["selected_columns"] = ["project_id"]

    snql_query = json_to_snql(query_body, "discover")
    query, _ = parse_snql_query(str(snql_query), dataset)

    assert query.get_from_clause().key == expected_entity
