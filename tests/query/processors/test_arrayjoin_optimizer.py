from datetime import datetime
from typing import Optional, Sequence

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entities.storage_selectors.selector import (
    DefaultQueryStorageSelector,
)
from snuba.datasets.factory import get_dataset
from snuba.datasets.plans.storage_plan_builder import StorageQueryPlanBuilder
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import arrayJoin, tupleElement
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.physical.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
    filter_key_values,
    filter_keys,
    get_filtered_mapping_keys,
    zip_columns,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query
from snuba.request import Request


def build_query(
    selected_columns: Optional[Sequence[Expression]] = None,
    condition: Optional[Expression] = None,
    having: Optional[Expression] = None,
) -> ClickhouseQuery:
    return ClickhouseQuery(
        None,
        selected_columns=[
            SelectedExpression(name=s.alias, expression=s)
            for s in selected_columns or []
        ],
        condition=condition,
        having=having,
    )


tags_filter_tests = [
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
            ],
        ),
        [],
        id="no tag filter",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag"),
            ),
        ),
        ["tag"],
        id="simple equality",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
            ],
            condition=in_condition(
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                [Literal(None, "tag1"), Literal(None, "tag2")],
            ),
        ),
        ["tag1", "tag2"],
        id="tag IN condition",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag"),
            ),
            having=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag2"),
            ),
        ),
        ["tag", "tag2"],
        id="conditions and having",
    ),
    pytest.param(
        build_query(
            selected_columns=[
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.OR,
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                in_condition(
                    FunctionCall(
                        "tags_key",
                        "arrayJoin",
                        (Column(None, None, "tags.key"),),
                    ),
                    [Literal(None, "tag1"), Literal(None, "tag2")],
                ),
            ),
            having=binary_condition(
                ConditionFunctions.EQ,
                FunctionCall(
                    "tags_key",
                    "arrayJoin",
                    (Column(None, None, "tags.key"),),
                ),
                Literal(None, "tag"),
            ),
        ),
        [],
        id="tag OR condition",
    ),
]


@pytest.mark.parametrize("query, expected_result", tags_filter_tests)
def test_get_filtered_mapping_keys(
    query: ClickhouseQuery,
    expected_result: Sequence[str],
) -> None:
    """
    Test the algorithm that identifies potential tag keys we can pre-filter
    through arrayFilter.
    """
    assert get_filtered_mapping_keys(query, "tags") == expected_result


def with_required(condition: Expression) -> Expression:
    return binary_condition(
        BooleanFunctions.AND,
        condition,
        binary_condition(
            BooleanFunctions.AND,
            FunctionCall(
                None,
                "greaterOrEquals",
                (
                    Column("_snuba_finish_ts", None, "finish_ts"),
                    Literal(None, datetime(2021, 1, 1, 0, 0)),
                ),
            ),
            binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "less",
                    (
                        Column("_snuba_finish_ts", None, "finish_ts"),
                        Literal(None, datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        Literal(None, 1),
                    ),
                ),
            ),
        ),
    )


test_data = [
    pytest.param(
        """
        MATCH (transactions)
        SELECT platform
        WHERE tags_key IN tuple('t1', 't2')
            AND finish_ts >= toDateTime('2021-01-01T00:00:00')
            AND finish_ts < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        ClickhouseQuery(
            None,
            selected_columns=[
                SelectedExpression(
                    name="col1", expression=Column("_snuba_col1", None, "col1")
                )
            ],
            condition=with_required(
                in_condition(
                    arrayJoin("_snuba_tags_key", Column(None, None, "tags.key")),
                    [Literal(None, "t1"), Literal(None, "t2")],
                )
            ),
        ),
        id="no tag in select clause",
    ),  # Individual tag, no change
    pytest.param(
        """
        MATCH (transactions)
        SELECT tags_key, tags_value
        WHERE release IN tuple('t1', 't2')
            AND finish_ts >= toDateTime('2021-01-01T00:00:00')
            AND finish_ts < toDateTime('2021-01-02T00:00:00')
            AND project_id = 1
        """,
        ClickhouseQuery(
            None,
            selected_columns=[
                SelectedExpression(
                    name="tags_key",
                    expression=tupleElement(
                        "_snuba_tags_key",
                        arrayJoin(
                            "snuba_all_tags",
                            zip_columns(
                                Column(None, None, "tags.key"),
                                Column(None, None, "tags.value"),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                ),
                SelectedExpression(
                    name="tags_value",
                    expression=tupleElement(
                        "_snuba_tags_value",
                        arrayJoin(
                            "snuba_all_tags",
                            zip_columns(
                                Column(None, None, "tags.key"),
                                Column(None, None, "tags.value"),
                            ),
                        ),
                        Literal(None, 2),
                    ),
                ),
            ],
            condition=with_required(
                in_condition(
                    Column("_snuba_release", None, "release"),
                    [Literal(None, "t1"), Literal(None, "t2")],
                )
            ),
        ),
        id="tags_key and tags_value in query no filter",
    ),  # tags_key and value in select. Zip keys and columns into an array.
    pytest.param(
        """
        MATCH (transactions)
        SELECT tags_key
        WHERE tags_key IN tuple('t1')
          AND finish_ts >= toDateTime('2021-01-01T00:00:00')
          AND finish_ts < toDateTime('2021-01-02T00:00:00')
          AND project_id = 1
        """,
        ClickhouseQuery(
            None,
            selected_columns=[
                SelectedExpression(
                    name="tags_key",
                    expression=arrayJoin(
                        "_snuba_tags_key",
                        filter_keys(
                            Column(None, None, "tags.key"), [Literal(None, "t1")]
                        ),
                    ),
                )
            ],
            condition=with_required(
                in_condition(
                    arrayJoin(
                        "_snuba_tags_key",
                        filter_keys(
                            Column(None, None, "tags.key"), [Literal(None, "t1")]
                        ),
                    ),
                    [Literal(None, "t1")],
                ),
            ),
        ),
        id="filter on keys only",
    ),  # Filtering tag keys. Apply arrayFilter into the arrayJoin.
    pytest.param(
        """
        MATCH (transactions)
        SELECT tags_key, tags_value
        WHERE tags_key IN tuple('t1')
          AND finish_ts >= toDateTime('2021-01-01T00:00:00')
          AND finish_ts < toDateTime('2021-01-02T00:00:00')
          AND project_id = 1
        """,
        ClickhouseQuery(
            None,
            selected_columns=[
                SelectedExpression(
                    name="tags_key",
                    expression=tupleElement(
                        "_snuba_tags_key",
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
                ),
                SelectedExpression(
                    name="tags_value",
                    expression=tupleElement(
                        "_snuba_tags_value",
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
                ),
            ],
            condition=with_required(
                in_condition(
                    tupleElement(
                        "_snuba_tags_key",
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
        ),
        id="filter on key value pars",
    ),  # tags_key and tags_value present together with conditions. Apply
    # arrayFilter over the zip between tags_key and tags_value
]


def parse_and_process(snql_query: str) -> ClickhouseQuery:
    dataset = get_dataset("transactions")
    query, snql_anonymized = parse_snql_query(str(snql_query), dataset)
    request = Request(
        id="a",
        original_body={"query": snql_query, "dataset": "transactions"},
        query=query,
        snql_anonymized=snql_anonymized,
        query_settings=HTTPQuerySettings(referrer="r"),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    from_clause = query.get_from_clause()
    assert isinstance(from_clause, QueryEntity)
    entity = get_entity(from_clause.key)
    storage = entity.get_writable_storage()
    assert storage is not None
    assert isinstance(query, LogicalQuery)
    for p in entity.get_query_processors():
        p.process_query(query, request.query_settings)

    query_plan = StorageQueryPlanBuilder(
        storages=list(entity.get_all_storage_connections()),
        selector=DefaultQueryStorageSelector(),
    ).build_and_rank_plans(query, request.query_settings)[0]

    ArrayJoinKeyValueOptimizer("tags").process_query(
        query_plan.query, request.query_settings
    )

    return query_plan.query


@pytest.mark.parametrize("query_body, expected_query", test_data)
def test_tags_processor(query_body: str, expected_query: ClickhouseQuery) -> None:
    """
    Tests the whole processing in some notable cases.
    """
    processed = parse_and_process(query_body)
    assert processed.get_selected_columns() == expected_query.get_selected_columns()
    assert processed.get_condition() == expected_query.get_condition()
    assert processed.get_having() == expected_query.get_having()


def test_formatting() -> None:
    """
    Validates the formatting of the arrayFilter expressions.
    """
    assert tupleElement(
        "tags_key",
        arrayJoin(
            "snuba_all_tags",
            zip_columns(
                Column(None, None, "tags.key"),
                Column(None, None, "tags.value"),
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(tupleElement((arrayJoin(arrayMap(x, y -> (x, y), "
        "tags.key, tags.value)) AS snuba_all_tags), 1) AS tags_key)"
    )

    assert tupleElement(
        "tags_key",
        arrayJoin(
            "snuba_all_tags",
            filter_key_values(
                zip_columns(
                    Column(None, None, "tags.key"),
                    Column(None, None, "tags.value"),
                ),
                [Literal(None, "t1"), Literal(None, "t2")],
            ),
        ),
        Literal(None, 1),
    ).accept(ClickhouseExpressionFormatter()) == (
        "(tupleElement((arrayJoin(arrayFilter(pair -> in("
        "tupleElement(pair, 1), ('t1', 't2')), "
        "arrayMap(x, y -> (x, y), tags.key, tags.value))) AS snuba_all_tags), 1) AS tags_key)"
    )


def test_aliasing() -> None:
    """
    Validates aliasing works properly when the query contains both tags_key
    and tags_value.
    """
    processed = parse_and_process(
        """
        MATCH (transactions)
        SELECT tags_value
        WHERE tags_key IN tuple('t1', 't2')
          AND project_id = 1
          AND finish_ts >= toDateTime('2021-01-01T00:00:00')
          AND finish_ts < toDateTime('2021-01-02T00:00:00')
        """
    )
    sql = format_query(processed).get_sql()
    transactions_table_name = (
        get_writable_storage(StorageKey.TRANSACTIONS)
        .get_table_writer()
        .get_schema()
        .get_table_name()
    )

    assert sql == (
        "SELECT (tupleElement((arrayJoin(arrayMap(x, y -> (x, y), "
        "tags.key, tags.value)) AS snuba_all_tags), 2) AS _snuba_tags_value) "
        f"FROM {transactions_table_name} "
        "WHERE in((tupleElement(snuba_all_tags, 1) AS _snuba_tags_key), ('t1', 't2')) "
        "AND equals((project_id AS _snuba_project_id), 1) "
        "AND greaterOrEquals((finish_ts AS _snuba_finish_ts), toDateTime('2021-01-01T00:00:00', 'Universal')) "
        "AND less(_snuba_finish_ts, toDateTime('2021-01-02T00:00:00', 'Universal')) "
        "LIMIT 1000 OFFSET 0"
    )
