import pytest

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.processors.tagsmap import NestedFieldConditionOptimizer
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings

test_data = [
    (
        {"conditions": [["d", "=", "1"], ["c", "=", "3"]]},
        [["d", "=", "1"], ["c", "=", "3"]],
    ),  # No tags
    (
        {"conditions": [["tags[test.tag]", "=", "1"], ["c", "=", "3"]]},
        [["c", "=", "3"], ["tags_map", "LIKE", "%|test.tag=1|%"]],
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
        [
            ["c", "=", "3"],
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
            ]
        },
        [
            ["c", "=", "3"],
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
            ]
        },
        [
            ["c", "=", "3"],
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
            ]
        },
        [
            ["c", "=", "3"],
            [["func", ["tags[test2.tag]"]], "=", "2"],
            ["tags_map", "LIKE", "%|test.tag=1|%"],
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
        [["c", "=", "3"], ["tags_map", "LIKE", "%|test.tag=1|%|test2.tag=2|%"]],
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
            ["tags_map", "LIKE", "%|test.tag=1|%"],
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
def test_nested_optimizer(query_body, expected_condition) -> None:
    state.set_config("optimize_nested_col_conditions", 1)
    query = Query(query_body, TableSource("my_table", ColumnSet([]), None, []))
    request_settings = RequestSettings(turbo=False, consistent=False, debug=False)
    processor = NestedFieldConditionOptimizer(
        nested_col="tags", flattened_col="tags_map"
    )
    processor.process_query(query, request_settings)
    assert query.get_conditions() == expected_condition
