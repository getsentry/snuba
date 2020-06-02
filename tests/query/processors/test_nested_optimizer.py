from datetime import datetime

import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.query.processors.tagsmap import NestedFieldConditionOptimizer
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        {
            "conditions": [
                ["d", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [["d", "=", "1"], ["c", "=", "3"], ["start_ts", ">", "2019-12-18T06:35:17"]],
    ),  # No tags
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
    ),  # One simple tag condition
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["finish_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["finish_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
    ),  # One simple tag condition, different timestamp
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-01-01T06:35:17"],
            ]
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-01-01T06:35:17"],
        ],
    ),  # Query start from before the existence of tagsmap
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-01-01T06:35:17"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-01-01T06:35:17"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
    ),  # Two start conditions: apply
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"],
        ],
    ),  # Multiple tags in the same merge
    (
        {
            "conditions": [
                ["tags[test2.tag]", "=", "2"],
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["tags[test3.tag]", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%|test3.tag=3|%"],
        ],
    ),  # Multiple tags in the same merge and properly sorted
    (
        {
            "conditions": [
                ["tags[test.tag]", "!=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "!=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test2.tag=2|%"],
            ["tags_map", "NOT LIKE", "%|test.tag=1|%"],
            ["tags_map", "NOT LIKE", "%|test3.tag=3|%"],
        ],
    ),  # Negative conditions mixed with positive ones
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["func", ["tags[test2.tag]"]], "=", "2"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            [["func", ["tags[test2.tag]"]], "=", "2"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
    ),  # Nested condition. Only the external one is converted
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["ifNull", ["tags[test2.tag]", ""]], "=", "2"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%"],
        ],
    ),  # Nested conditions in ifNull. This is converted.
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["contexts[test.context]", "=", "1"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ]
        },
        [
            ["c", "=", "3"],
            ["contexts[test.context]", "=", "1"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
        ],
    ),  # Both contexts and tags are present
    (
        {
            "conditions": [
                [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
                [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
            [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
    ),  # Nested conditions, ignored.
    (
        {
            "groupby": ["tags[another_tag]"],
            "having": [["tags[yet_another_tag]", "=", "1"]],
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
    ),  # Skip using flattened tags if the query requires tags unpacking anyway
    (
        {
            "orderby": ["tags[test.tag]"],
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["start_ts", ">", "2019-12-18T06:35:17"],
            ],
        },
        [
            ["tags[test.tag]", "=", "1"],
            ["c", "=", "3"],
            ["start_ts", ">", "2019-12-18T06:35:17"],
        ],
    ),  # Skip using flattened tags if the query requires tags unpacking anyway
]


@pytest.mark.parametrize("query_body, expected_condition", test_data)
def test_nested_optimizer(query_body, expected_condition) -> None:
    transactions = get_dataset("transactions")
    query = parse_query(query_body, transactions)
    request_settings = HTTPRequestSettings()
    request = Request("", query, request_settings, {}, "")

    query_plan = transactions.get_query_plan_builder().build_plan(request)
    processor = NestedFieldConditionOptimizer(
        nested_col="tags",
        flattened_col="tags_map",
        timestamp_cols={"start_ts", "finish_ts"},
        beginning_of_time=datetime(2019, 12, 11, 0, 0, 0),
    )
    clickhouse_query = query_plan.query
    processor.process_query(clickhouse_query, request_settings)

    assert clickhouse_query.get_conditions() == expected_condition
