from typing import Any, MutableMapping, Optional, Sequence, Set

import pytest

from snuba.clickhouse.astquery import AstSqlQuery
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import EntityKey, get_entity
from snuba.datasets.factory import get_dataset
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as SnubaQuery
from snuba.query.logical import SelectedExpression
from snuba.query.parser import parse_query
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
    filter_key_values,
    filter_keys,
    get_filtered_mapping_keys,
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
            selected_columns=[
                SelectedExpression(name=s.alias, expression=s)
                for s in selected_columns or []
            ],
            condition=condition,
            having=having,
        )
    )


tags_filter_tests = [
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key", "arrayJoin", (Column(None, None, "tags.key"),),
                ),
            ],
        ),
        set(),
        id="no tag filter",
    ),
    pytest.param(
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
        id="simple equality",
    ),
    pytest.param(
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
        id="tag IN condition",
    ),
    pytest.param(
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
        id="conditions and having",
    ),
    pytest.param(
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
        id="tag OR condition",
    ),
]


@pytest.mark.parametrize("query, expected_result", tags_filter_tests)
def test_get_filtered_mapping_keys(
    query: ClickhouseQuery, expected_result: Set[str],
) -> None:
    """
    Test the algorithm that identifies potential tag keys we can pre-filter
    through arrayFilter.
    """
    assert get_filtered_mapping_keys(query, "tags") == expected_result


test_data = [
    pytest.param(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["col1"],
            "conditions": [["tags_key", "IN", ["t1", "t2"]]],
        },
        build_query(
            selected_columns=[Column("col1", None, "col1")],
            condition=in_condition(
                None,
                arrayJoin("tags_key", Column(None, None, "tags.key")),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
        id="no tag in select clause",
    ),  # Individual tag, no change
    pytest.param(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["col", "IN", ["t1", "t2"]]],
        },
        build_query(
            selected_columns=[
                tupleElement(
                    "tags_key",
                    arrayJoin(
                        "snuba_all_tags",
                        zip_columns(
                            Column(None, None, "tags.key"),
                            Column(None, None, "tags.value"),
                        ),
                    ),
                    Literal(None, 1),
                ),
                tupleElement(
                    "tags_value",
                    arrayJoin(
                        "snuba_all_tags",
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
                Column("col", None, "col"),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
        id="tags_key and tags_value in query no filter",
    ),  # tags_key and value in select. Zip keys and columns into an array.
    pytest.param(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                arrayJoin(
                    "tags_key",
                    filter_keys(Column(None, None, "tags.key"), [Literal(None, "t1")]),
                ),
            ],
            condition=in_condition(
                None,
                arrayJoin(
                    "tags_key",
                    filter_keys(Column(None, None, "tags.key"), [Literal(None, "t1")]),
                ),
                [Literal(None, "t1")],
            ),
        ),
        id="filter on keys only",
    ),  # Filtering tag keys. Apply arrayFilter into the arrayJoin.
    pytest.param(
        {
            "aggregations": [],
            "groupby": [],
            "selected_columns": ["tags_key", "tags_value"],
            "conditions": [["tags_key", "IN", ["t1"]]],
        },
        build_query(
            selected_columns=[
                tupleElement(
                    "tags_key",
                    arrayJoin(
                        "snuba_all_tags",
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
                tupleElement(
                    "tags_value",
                    arrayJoin(
                        "snuba_all_tags",
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
                tupleElement(
                    "tags_key",
                    arrayJoin(
                        "snuba_all_tags",
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
        id="filter on key value pars",
    ),  # tags_key and tags_value present together with conditions. Apply
    # arrayFilter over the zip between tags_key and tags_value
]


def parse_and_process(query_body: MutableMapping[str, Any]) -> ClickhouseQuery:
    dataset = get_dataset("transactions")
    query = parse_query(query_body, dataset)
    request = Request("a", query, HTTPRequestSettings(), {}, "r")
    entity = get_entity(EntityKey(query.get_entity_name()))
    for p in entity.get_query_processors():
        p.process_query(query, request.settings)
    plan = entity.get_query_plan_builder().build_plan(request)

    ArrayJoinKeyValueOptimizer("tags").process_query(plan.query, request.settings)
    return plan.query


@pytest.mark.parametrize("query_body, expected_query", test_data)
def test_tags_processor(
    query_body: MutableMapping[str, Any], expected_query: ClickhouseQuery
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
    assert tupleElement(
        "tags_key",
        arrayJoin(
            "snuba_all_tags",
            zip_columns(
                Column(None, None, "tags.key"), Column(None, None, "tags.value"),
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(tupleElement((arrayJoin(arrayMap((x, y -> tuple(x, y)), "
        "tags.key, tags.value)) AS snuba_all_tags), 1) AS tags_key)"
    )

    assert tupleElement(
        "tags_key",
        arrayJoin(
            "snuba_all_tags",
            filter_key_values(
                zip_columns(
                    Column(None, None, "tags.key"), Column(None, None, "tags.value"),
                ),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(tupleElement((arrayJoin(arrayFilter((pair -> in("
        "tupleElement(pair, 1), tuple('t1', 't2'))), "
        "arrayMap((x, y -> tuple(x, y)), tags.key, tags.value))) AS snuba_all_tags), 1) AS tags_key)"
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
        "SELECT (tupleElement((arrayJoin(arrayMap((x, y -> tuple(x, y)), "
        "tags.key, tags.value)) AS snuba_all_tags), 2) AS tags_value) "
        "FROM transactions_local "
        "WHERE in((tupleElement(snuba_all_tags, 1) AS tags_key), tuple('t1', 't2'))"
    )
