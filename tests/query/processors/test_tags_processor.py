import pytest
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.query.processors.tags_processor import TagsProcessor
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        {
            "selected_columns": ["c1, c2, c3"],
            "conditions": [["c3", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["c1, c2, c3"],
            "conditions": [["c3", "IN", ["t1", "t2"]]],
        },
    ),  # No tag column
    (
        {
            "selected_columns": ["tags[t1]"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags[t1]"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
    ),  # Individual tag, no change
    (
        {
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
    ),  # Tags key in condition but only value in select. This could technically be
    # optimized but it would add more complexity
    (
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["col", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["col", "IN", ["t1", "t2"]]],
        },
    ),  # tags_key and value in select but no condition on it. No change
    (
        {
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags_key[t1, t2]"],
            "conditions": [["tags_key[t1, t2]", "IN", ["t1", "t2"]]],
        },
    ),  # tags_key in both select and condition. Apply change
    (
        {
            "selected_columns": ["tags_key"],
            "having": [["tags_key", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags_key[t1, t2]"],
            "having": [["tags_key[t1, t2]", "IN", ["t1", "t2"]]],
        },
    ),  # tags_key in having condition. Apply change
    (
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        {
            "selected_columns": ["tags_key[t1, t2]", "tags_value"],
            "conditions": [["tags_key[t1, t2]", "IN", ["t1", "t2"]]],
        },
    ),  # tags_key and value in select and condition. Apply change
    (
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                ["tags_key", "IN", ["t1", "t2"]],
                ["tags_key", "IN", ["t3", "t4"]],
                ["tags_key", "=", "t5"],
            ],
        },
        {
            "selected_columns": ["tags_key[t1, t2, t3, t4, t5]", "tags_value"],
            "conditions": [
                ["tags_key[t1, t2, t3, t4, t5]", "IN", ["t1", "t2"]],
                ["tags_key[t1, t2, t3, t4, t5]", "IN", ["t3", "t4"]],
                ["tags_key[t1, t2, t3, t4, t5]", "=", "t5"],
            ],
        },
    ),  # tags_key and value in select and condition. Multiple conditions. Merge them.
    # Technically we could remove the conditions in this case. Will do in a followup
    # change since it is complex on the old query infra.
    (
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                [["tags_key", "IN", ["t1", "t2"]], ["tags_key", "IN", ["t3", "t4"]]]
            ],
        },
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                [["tags_key", "IN", ["t1", "t2"]], ["tags_key", "IN", ["t3", "t4"]]]
            ],
        },
    ),  # Skip OR nested conditions
    (
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                ["tags_key", "IN", ["t1", "t2"]],
                [["tags_key", "IN", ["t3", "t4"]], ["tags_key", "=", "t5"]],
            ],
        },
        {
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                ["tags_key", "IN", ["t1", "t2"]],
                [["tags_key", "IN", ["t3", "t4"]], ["tags_key", "=", "t5"]],
            ],
        },
    ),  # Mixed case, some tags_key on top level some are not. Cannot do anything.
]


@pytest.mark.parametrize("query_body, expected_body", test_data)
def test_tags_processor(query_body, expected_body) -> None:
    query = parse_query(query_body, get_dataset("transactions"))
    request_settings = HTTPRequestSettings()
    processor = TagsProcessor()
    processor.process_query(query, request_settings)

    assert query.get_conditions() == expected_body.get("conditions")
    assert query.get_selected_columns() == expected_body.get("selected_columns")
