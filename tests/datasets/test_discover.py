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
    entity = get_entity(query.get_entity().key)
    for processor in entity.get_query_processors():
        processor.process_query(request.query, request.settings)

    plan = entity.get_query_plan_builder().build_plan(request)

    for physical_processor in plan.plan_processors:
        physical_processor.process_query(plan.query, request.settings)

    assert plan.query.get_from_clause().format_from() == expected_table, json.dumps(
        query_body
    )
