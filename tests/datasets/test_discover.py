from typing import Any, MutableMapping

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.factory import get_dataset, reset_dataset_factory
from snuba.query.data_source.simple import Entity as EntitySource
from snuba.query.snql.parser import parse_snql_query

reset_dataset_factory()

test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, EntityKey.DISCOVER_TRANSACTIONS),
    (
        {"conditions": [["type", "=", "transaction"], ["duration", ">", 1000]]},
        EntityKey.DISCOVER_TRANSACTIONS,
    ),
    ({"conditions": [["type", "=", "platform"]]}, EntityKey.DISCOVER_EVENTS),
    (
        {"conditions": [[["type", "=", "platform"], ["type", "=", "transaction"]]]},
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
                            ["equals", ["type", "release"]],
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
                            ["notEquals", ["type", "platform"]],
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
                            ["notEquals", ["type", "platform"]],
                        ],
                    ],
                    "=",
                    1,
                ]
            ]
        },
        EntityKey.DISCOVER_EVENTS,
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
                            ["notEquals", ["type", "platform"]],
                        ],
                    ],
                    "=",
                    1,
                ],
                ["duration", "=", 0],
            ]
        },
        EntityKey.DISCOVER_EVENTS,
    ),
    (
        {
            "conditions": [
                [
                    [
                        "and",
                        [
                            ["notEquals", ["type", "release"]],
                            ["notEquals", ["type", "platform"]],
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
    ({"selected_columns": ["span_id"]}, EntityKey.DISCOVER),
    ({"selected_columns": ["group_id", "span_id"]}, EntityKey.DISCOVER_EVENTS),
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
    (
        {"selected_columns": ["span_op_breakdowns[ops.http]"]},
        EntityKey.DISCOVER_TRANSACTIONS,
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

    request = json_to_snql(query_body, "discover")
    request.validate()
    query, _ = parse_snql_query(str(request.query), dataset)
    entity = query.get_from_clause()
    assert isinstance(entity, EntitySource)
    assert entity.key == expected_entity
