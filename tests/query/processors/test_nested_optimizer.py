import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.processors.tagsmap import CollapsedNestedFieldOptimizer
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings

test_data = [
    (
        {"conditions": [["d", "=", "1"], ["c", "=", "3"]]},
        [["d", "=", "1"], ["c", "=", "3"]],
    ),  # No tags
    (
        {"conditions": [["tags[test.tag]", "=", "1"], ["c", "=", "3"]]},
        [["c", "=", "3"], ["tags_map", "LIKE", "%test.tag:1%"]],
    ),  # One simple tag condition
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "=", "3"],
            ]
        },
        [["c", "=", "3"], ["tags_map", "LIKE", "%test.tag:1%test2.tag:2%test3.tag:3%"]],
    ),  # Multiple tags in the same merge
    (
        {
            "conditions": [
                ["tags[test.tag]", "!=", "1"],
                ["c", "=", "3"],
                ["tags[test2.tag]", "=", "2"],
                ["tags[test3.tag]", "!=", "3"],
            ]
        },
        [
            ["c", "=", "3"],
            ["tags_map", "LIKE", "%test2.tag:2%"],
            ["tags_map", "NOT LIKE", "%test.tag:1%"],
            ["tags_map", "NOT LIKE", "%test3.tag:3%"],
        ],
    ),  # Negative conditions mixed with positive ones
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["func", ["tags[test2.tag]"]], "=", "2"],
            ]
        },
        [
            ["c", "=", "3"],
            [["func", ["tags[test2.tag]"]], "=", "2"],
            ["tags_map", "LIKE", "%test.tag:1%"],
        ],
    ),  # Nested condition. Only the external one is converted
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                [["ifNull", ["tags[test2.tag]", ""]], "=", "2"],
            ]
        },
        [["c", "=", "3"], ["tags_map", "LIKE", "%test.tag:1%test2.tag:2%"]],
    ),  # Nested conditions in ifNull. This is converted.
    (
        {
            "conditions": [
                ["tags[test.tag]", "=", "1"],
                ["c", "=", "3"],
                ["contexts[test.context]", "=", "1"],
            ]
        },
        [
            ["c", "=", "3"],
            ["contexts[test.context]", "=", "1"],
            ["tags_map", "LIKE", "%test.tag:1%"],
        ],
    ),  # Both contexts and tags are present
    (
        {
            "conditions": [
                [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
                [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
            ],
        },
        [
            [["tags[test.tag]", "=", "1"], ["c", "=", "3"]],
            [["tags[test2.tag]", "=", "2"], ["tags[test3.tag]", "=", "3"]],
        ],
    ),  # Nested conditions, ignored.
]


@pytest.mark.parametrize("query_body, expected_condition", test_data)
def test_prewhere(query_body, expected_condition) -> None:
    query = Query(query_body, TableSource("my_table", ColumnSet([]), None, []))
    request_settings = RequestSettings(turbo=False, consistent=False, debug=False)
    processor = CollapsedNestedFieldOptimizer(nested_col="tags", merged_col="tags_map")
    processor.process_query(query, request_settings)
    assert query.get_conditions() == expected_condition
