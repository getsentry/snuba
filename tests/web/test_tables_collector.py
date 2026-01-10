from typing import Optional, Set, Union

import pytest

from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_inspector import TablesCollector
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal

ERRORS_SCHEMA = ColumnSet(
    [
        ("event_id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("group_id", UInt(32)),
    ]
)
GROUPS_SCHEMA = ColumnSet(
    [
        ("id", UInt(32)),
        ("project_id", UInt(32)),
        ("group_id", UInt(32)),
        ("message", String()),
    ]
)

SIMPLE_QUERY = ClickhouseQuery(
    Table(
        "errors_local",
        ERRORS_SCHEMA,
        final=True,
        sampling_rate=0.1,
        storage_key=StorageKey("events"),
    ),
    selected_columns=[
        SelectedExpression(
            "alias",
            FunctionCall("alias", "something", (Column(None, None, "event_id"),)),
        ),
        SelectedExpression(
            "group_id",
            Column(None, None, "group_id"),
        ),
    ],
    array_join=None,
    condition=binary_condition(
        ConditionFunctions.EQ,
        FunctionCall("alias", "tag", (Column(None, None, "group_id"),)),
        Literal(None, "1"),
    ),
    groupby=[FunctionCall("alias", "tag", (Column(None, None, "message"),))],
    prewhere=binary_condition(
        ConditionFunctions.EQ,
        FunctionCall("alias", "tag", (Column(None, None, "message"),)),
        Literal(None, "2"),
    ),
    having=None,
)

TEST_CASES = [
    pytest.param(
        SIMPLE_QUERY,
        {"errors_local"},
        True,
        0.1,
        id="Simple Query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=SIMPLE_QUERY,
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "something", (Column(None, None, "alias"),)),
                )
            ],
        ),
        {"errors_local"},
        True,
        None,
        id="Nested query. Count the inner query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(alias="err", data_source=SIMPLE_QUERY),
                right_node=IndividualNode(
                    alias="groups",
                    data_source=Table(
                        "groups_local", GROUPS_SCHEMA, storage_key=StorageKey("groups")
                    ),
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("err", "group_id"),
                        right=JoinConditionExpression("groups", "id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "event_id",
                    FunctionCall("alias", "something", (Column(None, "err", "alias"),)),
                ),
                SelectedExpression(
                    "group_id",
                    Column("group_id", "groups", "group_id"),
                ),
                SelectedExpression("message", Column("message", "groups", "message")),
            ],
        ),
        {"errors_local", "groups_local"},
        True,
        None,
        id="Join between a subquery and an individual table.",
    ),
]


@pytest.mark.parametrize(
    "query, expected_tables, expected_final, expected_sampling",
    TEST_CASES,
)
def test_count_columns(
    query: Union[ClickhouseQuery, CompositeQuery[Table]],
    expected_tables: Set[str],
    expected_final: bool,
    expected_sampling: Optional[float],
) -> None:
    tables_collector = TablesCollector()
    tables_collector.visit(query)
    assert tables_collector.get_tables() == expected_tables
    assert tables_collector.any_final() == expected_final
    assert tables_collector.get_sample_rate() == expected_sampling
