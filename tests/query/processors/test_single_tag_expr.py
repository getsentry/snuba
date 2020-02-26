import pytest

from snuba.clickhouse.columns import (
    ColumnSet,
    String,
    UInt,
)
from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.datasets.factory import get_dataset
from snuba.datasets.promoted_columns import PromotedColumnSpec
from snuba.query.conditions import binary_condition, ConditionFunctions, in_condition
from snuba.query.dsl import array_element
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.parser import parse_query
from snuba.query.processors.single_tag_expr import SingleTagProcessor
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        {
            "selected_columns": ["c1", "c2", "c3"],
            "aggregations": [],
            "groupby": [],
            "conditions": [["c3", "IN", ["t1", "t2"]]],
        },
        [Column(None, "c1", None), Column(None, "c2", None), Column(None, "c3", None)],
        [],
        in_condition(
            None, Column(None, "c3", None), [Literal(None, "t1"), Literal(None, "t2")],
        ),
        "SELECT c1, c2, c3 FROM test_sentry_local WHERE in(c3, tuple('t1', 't2'))",
    ),  # No tags, nothing explodes
    (
        {
            "selected_columns": ["tags[myTag]"],
            "aggregations": [],
            "groupby": [],
            "conditions": [],
        },
        [
            array_element(
                "tags[myTag]",
                Column(None, "tags.value", None),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "tags.key", None), Literal(None, "myTag")),
                ),
            )
        ],
        [],
        None,
        "SELECT (arrayElement(tags.value, indexOf(tags.key, 'myTag')) AS `tags[myTag]`) FROM test_sentry_local",
    ),  # One single not promoted tag
    (
        {
            "selected_columns": ["tags[myTag]"],
            "aggregations": [],
            "conditions": [["tags[myTag]", "=", "a"]],
        },
        [
            array_element(
                "tags[myTag]",
                Column(None, "tags.value", None),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "tags.key", None), Literal(None, "myTag")),
                ),
            )
        ],
        [],
        binary_condition(
            None,
            ConditionFunctions.EQ,
            array_element(
                "tags[myTag]",
                Column(None, "tags.value", None),
                FunctionCall(
                    None,
                    "indexOf",
                    (Column(None, "tags.key", None), Literal(None, "myTag")),
                ),
            ),
            Literal(None, "a"),
        ),
        "SELECT (arrayElement(tags.value, indexOf(tags.key, 'myTag')) AS `tags[myTag]`) FROM test_sentry_local WHERE equals(`tags[myTag]`, 'a')",
    ),  # Tag requested twice, use alias
    (
        {"selected_columns": ["tags[app.device]", "contexts[device.family]"]},
        [
            Column("tags[app.device]", "app_device", None),
            Column("contexts[device.family]", "device_family", None),
        ],
        [],
        None,
        "SELECT (app_device AS `tags[app.device]`), (device_family AS `contexts[device.family]`) FROM test_sentry_local",
    ),  # Promoted tag and promoted context
]


@pytest.mark.parametrize(
    "query_body, expect_select, expect_groupby, expect_condition, formatted_query",
    test_data,
)
def test_tags_processor(
    query_body, expect_select, expect_groupby, expect_condition, formatted_query
) -> None:
    dataset = get_dataset("events")
    query = parse_query(query_body, dataset)
    request_settings = HTTPRequestSettings()

    all_columns = ColumnSet(
        [
            ("app_device", String()),
            ("device", String()),
            ("device_family", String()),
            ("runtime", String()),
            ("runtime_name", String()),
            ("browser", String()),
            ("browser_name", String()),
            ("os", String()),
            ("os_name", String()),
            ("os_rooted", UInt(8)),
        ]
    )

    tags_column_map = {
        "tags": PromotedColumnSpec({"app.device": "app_device", "device": "device"}),
        "contexts": PromotedColumnSpec(
            {"device.family": "device_family", "runtime": "runtime"}
        ),
    }

    processor = SingleTagProcessor({"tags", "contexts"}, all_columns, tags_column_map)
    processor.process_query(query, request_settings)

    assert query.get_selected_columns_from_ast() == expect_select
    assert query.get_groupby_from_ast() == expect_groupby
    assert query.get_condition_from_ast() == expect_condition

    assert AstClickhouseQuery(query, request_settings).format_sql() == formatted_query
