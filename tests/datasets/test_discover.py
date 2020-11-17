import simplejson as json
import pytest
from typing import Any, MutableMapping

from snuba.datasets.factory import get_dataset
from snuba.datasets.entities.factory import get_entity
from snuba.query.parser import parse_query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    ({"conditions": [["type", "=", "transaction"]]}, "transactions_local"),
    (
        {"conditions": [["type", "=", "transaction"], ["duration", ">", 1000]]},
        "transactions_local",
    ),
    ({"conditions": [["type", "=", "error"]]}, "sentry_local"),
    (
        {"conditions": [[["type", "=", "error"], ["type", "=", "transaction"]]]},
        "sentry_local",
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
        "sentry_local",
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
        "transactions_local",
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
        "sentry_local",
    ),
    ({"conditions": [["type", "!=", "transaction"]]}, "sentry_local"),
    ({"conditions": []}, "sentry_local"),
    ({"conditions": [["duration", "=", 0]]}, "transactions_local"),
    (
        {"conditions": [["event_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        "transactions_local",
    ),
    (
        {"conditions": [["group_id", "=", "asdasdasd"], ["duration", "=", 0]]},
        "sentry_local",
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
        "transactions_local",
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
        "transactions_local",
    ),
    # No conditions, other referenced columns
    ({"selected_columns": ["group_id"]}, "sentry_local"),
    ({"selected_columns": ["trace_id"]}, "transactions_local"),
    ({"selected_columns": ["group_id", "trace_id"]}, "sentry_local"),
    ({"aggregations": [["max", "duration", "max_duration"]]}, "transactions_local"),
    (
        {"aggregations": [["apdex(duration, 300)", None, "apdex_duration_300"]]},
        "transactions_local",
    ),
    (
        {"aggregations": [["failure_rate()", None, "failure_rate"]]},
        "transactions_local",
    ),
    ({"aggregations": [["isHandled()", None, "handled"]]}, "sentry_local",),
    ({"aggregations": [["notHandled()", None, "handled"]]}, "sentry_local",),
    ({"selected_columns": ["measurements[lcp.elementSize]"]}, "transactions_local"),
]


@pytest.mark.parametrize("query_body, expected_table", test_data)
def test_data_source(
    query_body: MutableMapping[str, Any], expected_table: str,
) -> None:
    request_settings = HTTPRequestSettings()
    dataset = get_dataset("discover")
    query = parse_query(query_body, dataset)
    request = Request("a", query, request_settings, {}, "r")
    entity = get_entity(query.get_from_clause().key)

    pipeline = entity.get_query_pipeline_builder().build_processing_pipeline(
        request.query, request.settings
    )
    plan = pipeline.execute()

    assert plan.query.get_from_clause().table_name == expected_table, json.dumps(
        query_body
    )
