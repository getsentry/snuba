from typing import Any, MutableMapping

import pytest
from snuba.datasets.entities import EntityKey
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query

test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, EntityKey.DISCOVER_TRANSACTIONS),
    (
        {"conditions": [["type", "=", "transaction"], ["duration", ">", 1000]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    ({"conditions": [["type", "=", "error"]]}, EntityKey.DISCOVER_EVENTS),
    (
        {"conditions": [[["type", "=", "error"], ["type", "=", "transaction"]]]},
        EntityKey.DISCOVER,
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
        EntityKey.DISCOVER,
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
        EntityKey.DISCOVER_TRANSACTIONS,
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
        EntityKey.DISCOVER,
    ),
    ({"conditions": [["type", "!=", "transaction"]]}, EntityKey.DISCOVER_EVENTS),
    ({"conditions": []}, EntityKey.DISCOVER),
    ({"conditions": [["duration", "=", 0]]}, EntityKey.DISCOVER_TRANSACTIONS),
    (
        {"conditions": [["event_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    (
        {"conditions": [["group_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        EntityKey.DISCOVER,
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
        EntityKey.DISCOVER_TRANSACTIONS,
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
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    # # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, EntityKey.DISCOVER_EVENTS),
    ({"selected_columns": ["trace_id"]}, EntityKey.DISCOVER_TRANSACTIONS),
    ({"selected_columns": ["group_id", "trace_id"]}, EntityKey.DISCOVER),
    (
        {"aggregations": [["max", "duration", "max_duration"]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    (
        {"aggregations": [["apdex(duration, 300)", None, "apdex_duration_300"]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    (
        {"aggregations": [["failure_rate()", None, "failure_rate"]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    ({"aggregations": [["isHandled()", None, "handled"]]}, EntityKey.DISCOVER_EVENTS),
    ({"aggregations": [["notHandled()", None, "handled"]]}, EntityKey.DISCOVER_EVENTS),
    (
        {"selected_columns": ["measurements[lcp.elementSize]"]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
]


@pytest.mark.parametrize("query_body, expected_entity", test_data)
def test_data_source(
    query_body: MutableMapping[str, Any], expected_entity: EntityKey,
) -> None:
    dataset = get_dataset("discover")
    query = parse_query(query_body, dataset)

    assert query.get_from_clause().key == expected_entity
