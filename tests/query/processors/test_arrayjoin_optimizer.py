import pytest

from typing import Any, MutableMapping

from snuba.clickhouse.astquery import AstSqlQuery
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings
from snuba.query.processors.arrayjoin_optimizer import ArrayjoinOptimizer
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.logical import Query as SnubaQuery
from snuba.query.expressions import FunctionCall, Column, Lambda, Argument, Literal
from snuba.query.conditions import in_condition


all_tags_expr = FunctionCall(
    "all_tags",
    "arrayJoin",
    (
        FunctionCall(
            None,
            "arrayMap",
            (
                Lambda(
                    None,
                    ("x", "y"),
                    FunctionCall(
                        None, "tuple", (Argument(None, "x"), Argument(None, "y"),),
                    ),
                ),
                Column(None, None, "tags.key"),
                Column(None, None, "tags.value"),
            ),
        ),
    ),
)

test_data = [
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["col1"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        ClickhouseQuery(
            SnubaQuery(
                {},
                None,
                selected_columns=[Column(None, None, "col1")],
                condition=in_condition(
                    None,
                    FunctionCall(
                        "tags_key", "arrayJoin", (Column(None, None, "tags.key"),)
                    ),
                    [Literal(None, "t1"), Literal(None, "t2")],
                ),
            )
        ),
    ),  # Individual tag, no change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        ClickhouseQuery(
            SnubaQuery(
                {},
                None,
                selected_columns=[
                    FunctionCall(
                        "tags_value",
                        "arrayElement",
                        (all_tags_expr, Literal(None, 2),),
                    )
                ],
                condition=in_condition(
                    None,
                    FunctionCall(
                        "tags_key", "arrayElement", (all_tags_expr, Literal(None, 1),),
                    ),
                    [Literal(None, "t1"), Literal(None, "t2")],
                ),
            )
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
        ClickhouseQuery(
            SnubaQuery(
                {},
                None,
                selected_columns=[
                    FunctionCall(
                        "tags_key", "arrayElement", (all_tags_expr, Literal(None, 1)),
                    ),
                    FunctionCall(
                        "tags_value", "arrayElement", (all_tags_expr, Literal(None, 2)),
                    ),
                ],
                condition=in_condition(
                    None,
                    Column(None, None, "col"),
                    [Literal(None, "t1"), Literal(None, "t2")],
                ),
            )
        ),
    ),  # tags_key and value in select but no condition on it. No change
    (
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        ClickhouseQuery(
            SnubaQuery(
                {},
                None,
                selected_columns=[
                    FunctionCall(
                        "tags_key", "arrayJoin", (Column(None, None, "tags.key"),)
                    )
                ],
                condition=in_condition(
                    None,
                    FunctionCall(
                        "tags_key", "arrayJoin", (Column(None, None, "tags.key"),)
                    ),
                    [Literal(None, "t1"), Literal(None, "t2")],
                ),
            )
        ),
    ),  # tags_key in both select and condition. Apply change
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
    assert plan.query.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert (
        plan.query.get_condition_from_ast() == expected_query.get_condition_from_ast()
    )
    assert (
        plan.query.get_arrayjoin_from_ast() == expected_query.get_arrayjoin_from_ast()
    )
    assert plan.query.get_having_from_ast() == expected_query.get_having_from_ast()
    assert plan.query.get_orderby_from_ast() == expected_query.get_orderby_from_ast()


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
        "SELECT (arrayElement((arrayJoin(arrayMap((x, y -> tuple(x, y)), "
        "tags.key, tags.value)) AS all_tags), 2) AS tags_value) "
        "FROM test_transactions_local "
        "WHERE in((arrayElement(all_tags, 1) AS tags_key), tuple('t1', 't2'))"
    )
