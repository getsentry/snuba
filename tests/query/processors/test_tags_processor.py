from typing import Any, MutableMapping

import pytest

from snuba.clickhouse.dictquery import DictSqlQuery
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        {
            "selected_columns": ["c1", "c2", "c3"],
            "aggregations": [],
            "groupby": [],
            "conditions": [["c3", "IN", ["t1", "t2"]]],
        },
        "SELECT c1, c2, c3 FROM transactions_local WHERE c3 IN ('t1', 't2')",
    ),
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags[t1]"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (tags.value[indexOf(tags.key, 't1')] AS `tags[t1]`) "
            "FROM transactions_local "
            "WHERE (arrayJoin(tags.key) AS tags_key) IN ('t1', 't2')"
        ),
    ),  # Individual tag, no change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) "
            "AS all_tags))[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE ((all_tags)[1] AS tags_key) IN ('t1', 't2')"
        ),
    ),  # Tags key in condition but only value in select. This could technically be
    # optimized but it would add more complexity
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["col", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) AS all_tags))[1] "
            "AS tags_key), ((all_tags)[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE col IN ('t1', 't2')"
        ),
    ),  # tags_key and value in select but no condition on it. No change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (arrayJoin(arrayFilter(tag -> tag IN ('t1','t2'), tags.key)) AS tags_key) "
            "FROM transactions_local "
            "WHERE tags_key IN ('t1', 't2')"
        ),
    ),  # tags_key in both select and condition. Apply change
    (
        {
            "aggregations": [],
            "groupby": ["tags_key"],
            "selected_columns": ["tags_key"],
            "having": [["tags_key", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (arrayJoin(arrayFilter(tag -> tag IN ('t1','t2'), tags.key)) AS tags_key), tags_key "
            "FROM transactions_local "
            "GROUP BY (tags_key) "
            "HAVING tags_key IN ('t1', 't2')"
        ),
    ),  # tags_key in having condition. Apply change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        (
            "SELECT (((arrayJoin(arrayFilter(pair -> pair[1] IN ('t1','t2'), "
            "arrayMap((x,y) -> [x,y], tags.key, tags.value))) AS all_tags))[1] AS tags_key), "
            "((all_tags)[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE tags_key IN ('t1', 't2')"
        ),
    ),  # tags_key and value in select and condition. Apply change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                ["tags_key", "IN", ["t1", "t2"]],
                ["tags_key", "IN", ["t3", "t4"]],
                ["tags_key", "=", "t5"],
            ],
        },
        (
            "SELECT (((arrayJoin(arrayFilter(pair -> pair[1] IN ('t1','t2','t3','t4','t5'), "
            "arrayMap((x,y) -> [x,y], tags.key, tags.value))) AS all_tags))[1] AS tags_key), "
            "((all_tags)[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE tags_key IN ('t1', 't2') AND "
            "tags_key IN ('t3', 't4') AND "
            "tags_key = 't5'"
        ),
    ),  # tags_key and value in select and condition. Multiple conditions. Merge them.
    # Technically we could remove the conditions in this case. Will do in a followup
    # change since it is complex on the old query infra.
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                [["tags_key", "IN", ["t1", "t2"]], ["tags_key", "IN", ["t3", "t4"]]]
            ],
        },
        (
            "SELECT (((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) AS all_tags))[1] "
            "AS tags_key), ((all_tags)[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE (tags_key IN ('t1', 't2') OR tags_key IN ('t3', 't4'))"
        ),
    ),  # Skip OR nested conditions
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [
                ["tags_key", "IN", ["t1", "t2"]],
                [["tags_key", "IN", ["t3", "t4"]], ["tags_key", "=", "t5"]],
            ],
        },
        (
            "SELECT (((arrayJoin(arrayMap((x,y) -> [x,y], tags.key, tags.value)) AS all_tags))[1] "
            "AS tags_key), ((all_tags)[2] AS tags_value) "
            "FROM transactions_local "
            "WHERE tags_key IN ('t1', 't2') AND (tags_key IN ('t3', 't4') OR tags_key = 't5')"
        ),
    ),  # Mixed case, some tags_key on top level some are not. Cannot do anything.
]


@pytest.mark.parametrize("query_body, expected_query", test_data)
def test_tags_processor(
    query_body: MutableMapping[str, Any], expected_query: str
) -> None:
    # TODO: build a reusable framework to trigger the the query
    # processing pipeline for a dataset.
    dataset = get_dataset("transactions")
    query = parse_query(query_body, dataset)
    request_settings = HTTPRequestSettings()
    request = Request("a", query, request_settings, {}, "r")
    for p in dataset.get_query_processors():
        p.process_query(query, request_settings)
    plan = dataset.get_query_plan_builder().build_plan(request)
    ArrayJoinKeyValueOptimizer("tags").process_query(plan.query, request.settings)

    assert (
        DictSqlQuery(dataset, plan.query, request_settings).format_sql()
        == expected_query
    )
