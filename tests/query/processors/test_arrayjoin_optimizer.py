from typing import Any, MutableMapping, Optional, Sequence, Set

import pytest

from snuba.clickhouse.astquery import AstSqlQuery
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.processors.arrayjoin_expressions import (
    tag_column,
    filter_pairs,
    filter_tag,
    value_column,
    array_join,
    map_columns,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as SnubaQuery
from snuba.query.parser import parse_query
from snuba.query.processors.arrayjoin_optimizer import (
    ArrayjoinOptimizer,
    array_join,
    filter_pairs,
    filter_tag,
    get_filter_tags,
    map_columns,
    tag_column,
    value_column,
)
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings


def build_query(
    selected_columns: Optional[Sequence[Expression]], condition: Optional[Expression]
) -> ClickhouseQuery:
    return ClickhouseQuery(
        SnubaQuery({}, None, selected_columns=selected_columns, condition=condition,)
    )


tags_filter_tests = [
    (
        "no tag filter",
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
            ],
            condition=None,
        ),
        set(),
    ),
    (
        "simple equality",
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
            ],
            condition=binary_condition(
                None,
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag"),
            ),
        ),
        {"tag"},
    ),
    (
        "tag IN condition",
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
            ],
            condition=in_condition(
                None,
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
                [Literal(None, "tag1"), Literal(None, "tag2")],
            ),
        ),
        {"tag1", "tag2"},
    ),
    (
        "tag OR condition",
        build_query(
            selected_columns=None,
            condition=binary_condition(
                None,
                BooleanFunctions.OR,
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
                in_condition(
                    None,
                    FunctionCall(
                        "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                    ),
                    [Literal(None, "tag1"), Literal(None, "tag2")],
                ),
            ),
        ),
        set(),
    ),
]


@pytest.mark.parametrize("name, query, expected_result", tags_filter_tests)
def test_get_filter_tags(
    name: str, query: ClickhouseQuery, expected_result: Set[str],
) -> None:
    assert get_filter_tags(query) == expected_result


test_data = [
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["col1"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        build_query(
            selected_columns=[Column(None, None, "col1")],
            condition=in_condition(
                None,
                array_join("tags_key", Column(None, None, "tags.key")),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
    ),  # Individual tag, no change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        build_query(
            selected_columns=[value_column("tags_value", map_columns())],
            condition=in_condition(
                None,
                tag_column("tags_key", map_columns()),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
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
        build_query(
            selected_columns=[
                tag_column("tags_key", map_columns()),
                value_column("tags_value", map_columns()),
            ],
            condition=in_condition(
                None,
                Column(None, None, "col"),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
    ),  # tags_key and value in select but no condition on it. No change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                array_join("tags_key", filter_tag([Literal(None, "t1")])),
            ],
            condition=in_condition(
                None,
                array_join("tags_key", filter_tag([Literal(None, "t1")])),
                [Literal(None, "t1")],
            ),
        ),
    ),
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                tag_column("tags_key", filter_pairs([Literal(None, "t1")])),
                value_column("tags_value", filter_pairs([Literal(None, "t1")])),
            ],
            condition=in_condition(
                None,
                tag_column("tags_key", filter_pairs([Literal(None, "t1")])),
                [Literal(None, "t1")],
            ),
        ),
    ),
]


@pytest.mark.parametrize("query_body, expected_query", test_data)
def test_tags_processor(
    query_body: MutableMapping[str, Any], expected_query: ClickhouseQuery
) -> None:
    dataset = get_dataset("transactions")
    query = parse_query(query_body, dataset)
    request_settings = HTTPRequestSettings()
    request = Request("a", query, request_settings, {}, "r")
    for p in dataset.get_query_processors():
        p.process_query(query, request.settings)
    plan = dataset.get_query_plan_builder().build_plan(request)

    ArrayjoinOptimizer().process_query(plan.query, request.settings)
    # We cannot just run == on the query objects. The content of the two
    # objects is different, being one the AST and the ont the AST + raw body
    assert (
        plan.query.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert (
        plan.query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    )


def test_formatting() -> None:
    assert tag_column("tags_key", map_columns()).accept(
        ClickhouseExpressionFormatter()
    ) == (
        "(arrayElement((arrayJoin(arrayMap((x, y -> array(x, y)), "
        "tags.key, tags.value)) AS all_tags), 1) AS tags_key)"
    )

    assert tag_column(
        "tags_key", filter_pairs([Literal(None, "t1"), Literal(None, "t2")])
    ).accept(ClickhouseExpressionFormatter()) == (
        "(arrayElement((arrayJoin(arrayFilter((pair -> in("
        "arrayElement(pair, 1), tuple('t1', 't2'))), "
        "arrayMap((x, y -> array(x, y)), tags.key, tags.value))) AS all_tags), 1) AS tags_key)"
    )


def test_aliasing() -> None:
    dataset = get_dataset("transactions")
    query = parse_query(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        dataset,
    )
    request_settings = HTTPRequestSettings()
    request = Request("a", query, request_settings, {}, "r")
    for p in dataset.get_query_processors():
        p.process_query(query, request.settings)
    plan = dataset.get_query_plan_builder().build_plan(request)
    ArrayjoinOptimizer().process_query(plan.query, request.settings)
    sql = AstSqlQuery(plan.query, request.settings).format_sql()

    assert sql == (
        "SELECT (arrayElement((arrayJoin(arrayMap((x, y -> array(x, y)), "
        "tags.key, tags.value)) AS all_tags), 2) AS tags_value) "
        "FROM test_transactions_local "
        "WHERE in((arrayElement(all_tags, 1) AS tags_key), tuple('t1', 't2'))"
    )
