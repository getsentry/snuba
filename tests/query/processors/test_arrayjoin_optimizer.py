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
from snuba.query.dsl import arrayElement
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as SnubaQuery
from snuba.query.parser import parse_query
from snuba.query.processors.arrayjoin_optimizer import (
    ArrayjoinOptimizer,
    array_join,
    filter_key_values,
    filter_keys,
    get_filtered_tag_keys,
    zip_columns,
)
from snuba.request import Request
from snuba.request.request_settings import HTTPRequestSettings


def build_query(
    selected_columns: Optional[Sequence[Expression]] = None,
    condition: Optional[Expression] = None,
    having: Optional[Expression] = None,
) -> ClickhouseQuery:
    return ClickhouseQuery(
        SnubaQuery(
            {},
            None,
            selected_columns=selected_columns,
            condition=condition,
            having=having,
        )
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
        "conditions and having",
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
            having=binary_condition(
                None,
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag2"),
            ),
        ),
        {"tag", "tag2"},
    ),
    (
        "tag OR condition",
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
            ],
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
            having=binary_condition(
                None,
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag"),
            ),
        ),
        set(),
    ),
]


@pytest.mark.parametrize("name, query, expected_result", tags_filter_tests)
def test_get_filtered_tag_keys(
    name: str, query: ClickhouseQuery, expected_result: Set[str],
) -> None:
    """
    Test the algorithm that identifies potential tag keys we can pre-filter
    through arrayFilter.
    """
    assert get_filtered_tag_keys(query) == expected_result


test_data = [
    (
        "no tag in select clause",
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
        "tags_key and tags_value in query no filter",
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["col", "IN", ["t1", "t2"]]],
        },
        build_query(
            selected_columns=[
                arrayElement(
                    "tags_key",
                    array_join(
                        "all_tags",
                        zip_columns(
                            Column(None, None, "tags.key"),
                            Column(None, None, "tags.value"),
                        ),
                    ),
                    Literal(None, 1),
                ),
                arrayElement(
                    "tags_value",
                    array_join(
                        "all_tags",
                        zip_columns(
                            Column(None, None, "tags.key"),
                            Column(None, None, "tags.value"),
                        ),
                    ),
                    Literal(None, 2),
                ),
            ],
            condition=in_condition(
                None,
                Column(None, None, "col"),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
    ),  # tags_key and value in select but no condition on it. No change
    (
        "filter on keys only",
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                array_join(
                    "tags_key",
                    filter_keys(Column(None, None, "tags.key"), [Literal(None, "t1")]),
                ),
            ],
            condition=in_condition(
                None,
                array_join(
                    "tags_key",
                    filter_keys(Column(None, None, "tags.key"), [Literal(None, "t1")]),
                ),
                [Literal(None, "t1")],
            ),
        ),
    ),
    (
        "filter on key value pars",
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                arrayElement(
                    "tags_key",
                    array_join(
                        "all_tags",
                        filter_key_values(
                            zip_columns(
                                Column(None, None, "tags.key"),
                                Column(None, None, "tags.value"),
                            ),
                            [Literal(None, "t1")],
                        ),
                    ),
                    Literal(None, 1),
                ),
                arrayElement(
                    "tags_value",
                    array_join(
                        "all_tags",
                        filter_key_values(
                            zip_columns(
                                Column(None, None, "tags.key"),
                                Column(None, None, "tags.value"),
                            ),
                            [Literal(None, "t1")],
                        ),
                    ),
                    Literal(None, 2),
                ),
            ],
            condition=in_condition(
                None,
                arrayElement(
                    "tags_key",
                    array_join(
                        "all_tags",
                        filter_key_values(
                            zip_columns(
                                Column(None, None, "tags.key"),
                                Column(None, None, "tags.value"),
                            ),
                            [Literal(None, "t1")],
                        ),
                    ),
                    Literal(None, 1),
                ),
                [Literal(None, "t1")],
            ),
        ),
    ),
]


def parse_and_process(query_body: MutableMapping[str, Any]) -> ClickhouseQuery:
    dataset = get_dataset("transactions")
    query = parse_query(query_body, dataset)
    request = Request("a", query, HTTPRequestSettings(), {}, "r")
    for p in dataset.get_query_processors():
        p.process_query(query, request.settings)
    plan = dataset.get_query_plan_builder().build_plan(request)

    ArrayjoinOptimizer().process_query(plan.query, request.settings)
    return plan.query


@pytest.mark.parametrize("name, query_body, expected_query", test_data)
def test_tags_processor(
    name: str, query_body: MutableMapping[str, Any], expected_query: ClickhouseQuery
) -> None:
    """
    Tests the whole processing in some notable cases.
    """
    processed = parse_and_process(query_body)
    assert (
        processed.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert processed.get_condition_from_ast() == expected_query.get_condition_from_ast()
    assert processed.get_having_from_ast() == expected_query.get_having_from_ast()


def test_formatting() -> None:
    """
    Validates the formatting of the arrayFilter expressions.
    """
    assert arrayElement(
        "tags_key",
        array_join(
            "all_tags",
            zip_columns(
                Column(None, None, "tags.key"), Column(None, None, "tags.value"),
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(arrayElement((arrayJoin(arrayMap((x, y -> array(x, y)), "
        "tags.key, tags.value)) AS all_tags), 1) AS tags_key)"
    )

    assert arrayElement(
        "tags_key",
        array_join(
            "all_tags",
            filter_key_values(
                zip_columns(
                    Column(None, None, "tags.key"), Column(None, None, "tags.value"),
                ),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(arrayElement((arrayJoin(arrayFilter((pair -> in("
        "arrayElement(pair, 1), tuple('t1', 't2'))), "
        "arrayMap((x, y -> array(x, y)), tags.key, tags.value))) AS all_tags), 1) AS tags_key)"
    )


def test_aliasing() -> None:
    """
    Validates aliasing works properly when the query contains both tags_key
    and tags_value.
    """
    processed = parse_and_process(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_value"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        }
    )
    sql = AstSqlQuery(processed, HTTPRequestSettings()).format_sql()

    assert sql == (
        "SELECT (arrayElement((arrayJoin(arrayMap((x, y -> array(x, y)), "
        "tags.key, tags.value)) AS all_tags), 2) AS tags_value) "
        "FROM test_transactions_local "
        "WHERE in((arrayElement(all_tags, 1) AS tags_key), tuple('t1', 't2'))"
    )
