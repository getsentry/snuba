from __future__ import annotations

import re
from datetime import datetime
from typing import cast

import pytest

from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import (
    get_entity,
    override_entity_map,
    reset_entity_factory,
)
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import arrayElement, divide, literals_tuple, multiply, plus
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query
from snuba.query.logical import Query as LogicalQuery
from snuba.query.mql.parser import parse_mql_query
from tests.query.joins.equivalence_schema import (
    EVENTS_SCHEMA,
    GROUPS_ASSIGNEE,
    GROUPS_SCHEMA,
    Events,
    GroupAssignee,
    GroupedMessage,
)
from tests.query.joins.join_structures import (
    events_groups_join,
    events_node,
    groups_node,
)

BASIC_JOIN = JoinClause(
    left_node=IndividualNode(
        alias="ev",
        data_source=Entity(
            EntityKey.EVENTS, EntityColumnSet(EVENTS_SCHEMA.columns), None
        ),
    ),
    right_node=IndividualNode(
        alias="gr",
        data_source=Entity(
            EntityKey.GROUPEDMESSAGE, EntityColumnSet(GROUPS_SCHEMA.columns), None
        ),
    ),
    keys=[
        JoinCondition(
            left=JoinConditionExpression("ev", "group_id"),
            right=JoinConditionExpression("gr", "id"),
        )
    ],
    join_type=JoinType.INNER,
)

TEST_CASES = [
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[],
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))],
                ),
            ),
            selected_columns=[],
        ),
        id="Basic join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression(
                    "count_ev_id",
                    FunctionCall(
                        "_snuba_count_ev_id",
                        "count",
                        (Column("_snuba_ev.event_id", "ev", "event_id"),),
                    ),
                ),
                SelectedExpression("group_id", Column("_snuba_group_id", "gr", "id")),
            ],
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        SelectedExpression(
                            "_snuba_ev.event_id",
                            Column("_snuba_ev.event_id", None, "event_id"),
                        ),
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                groups_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "id"),
                        ),
                        SelectedExpression(
                            "_snuba_id", Column("_snuba_id", None, "id")
                        ),
                    ],
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "count_ev_id",
                    FunctionCall(
                        "_snuba_count_ev_id",
                        "count",
                        (Column("_snuba_ev.event_id", "ev", "_snuba_ev.event_id"),),
                    ),
                ),
                SelectedExpression(
                    "group_id", Column("_snuba_group_id", "gr", "_snuba_group_id")
                ),
            ],
        ),
        id="Basic join with select",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression("group_id", Column("_snuba_group_id", "gr", "id")),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_group_id", "gr", "id"),
                Literal(None, 123),
            ),
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ],
                ),
                groups_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "id"),
                        ),
                        SelectedExpression(
                            "_snuba_id", Column("_snuba_id", None, "id")
                        ),
                    ],
                    condition=binary_condition(
                        ConditionFunctions.EQ,
                        Column("_snuba_group_id", None, "id"),
                        Literal(None, 123),
                    ),
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", Column("_snuba_group_id", "gr", "_snuba_group_id")
                ),
            ],
        ),
        id="Query with condition",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression("group_id", Column("_snuba_group_id", "gr", "id")),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column("_snuba_group_id", "gr", "id"),
                    Literal(None, 123),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column("_snuba_group_id", "gr", "id"),
                    FunctionCall(
                        None, "f", (Column("_snuba_e_group_id", "ev", "group_id"),)
                    ),
                ),
            ),
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        SelectedExpression(
                            "_snuba_gen_1",
                            FunctionCall(
                                "_snuba_gen_1",
                                "f",
                                (Column("_snuba_e_group_id", None, "group_id"),),
                            ),
                        ),
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ],
                ),
                groups_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "id"),
                        ),
                        SelectedExpression(
                            "_snuba_id", Column("_snuba_id", None, "id")
                        ),
                    ],
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column("_snuba_group_id", None, "id"),
                        Literal(None, 123),
                    ),
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", Column("_snuba_group_id", "gr", "_snuba_group_id")
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                Column("_snuba_group_id", "gr", "_snuba_group_id"),
                Column("_snuba_gen_1", "ev", "_snuba_gen_1"),
            ),
        ),
        id="Query with condition across entities",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=BASIC_JOIN,
                right_node=IndividualNode(
                    "as", Entity(EntityKey.GROUPASSIGNEE, GROUPS_ASSIGNEE, None)
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "group_id"),
                        right=JoinConditionExpression("as", "group_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression("group_id", Column("_snuba_group_id", "gr", "id")),
            ],
        ),
        CompositeQuery(
            from_clause=JoinClause(
                left_node=events_groups_join(
                    events_node(
                        [
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "group_id"),
                            ),
                        ],
                    ),
                    groups_node(
                        [
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "id"),
                            ),
                            SelectedExpression(
                                "_snuba_id", Column("_snuba_id", None, "id")
                            ),
                        ],
                    ),
                ),
                right_node=IndividualNode(
                    alias="as",
                    data_source=LogicalQuery(
                        from_clause=Entity(EntityKey.GROUPASSIGNEE, GROUPS_ASSIGNEE),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "group_id"),
                            ),
                        ],
                    ),
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "_snuba_group_id"),
                        right=JoinConditionExpression("as", "_snuba_group_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", Column("_snuba_group_id", "gr", "_snuba_group_id")
                ),
            ],
        ),
        id="Multi entity join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression(
                    "project_id",
                    FunctionCall(
                        "_snuba_project_id",
                        "f",
                        (Column("_snuba_project_id", "ev", "project_id"),),
                    ),
                ),
                SelectedExpression(
                    "max_timestamp",
                    FunctionCall(
                        "_snuba_max_timestamp",
                        "max",
                        (Column(None, "ev", "timestamp"),),
                    ),
                ),
            ],
            groupby=[
                FunctionCall(
                    "_snuba_project_id",
                    "f",
                    (Column("_snuba_project_id", "ev", "project_id"),),
                ),
            ],
            having=FunctionCall(
                None,
                "greater",
                (
                    FunctionCall(None, "min", (Column(None, "ev", "timestamp"),)),
                    Literal(None, "sometime"),
                ),
            ),
            granularity=123,
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        # TODO: We should deduplicate the next two expressions
                        SelectedExpression(
                            "_snuba_gen_1", Column("_snuba_gen_1", None, "timestamp")
                        ),
                        SelectedExpression(
                            "_snuba_gen_2", Column("_snuba_gen_2", None, "timestamp")
                        ),
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                        SelectedExpression(
                            "_snuba_project_id",
                            FunctionCall(
                                "_snuba_project_id",
                                "f",
                                (Column("_snuba_project_id", None, "project_id"),),
                            ),
                        ),
                    ],
                    granularity=123,
                ),
                groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))],
                    granularity=123,
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "project_id",
                    Column("_snuba_project_id", "ev", "_snuba_project_id"),
                ),
                SelectedExpression(
                    "max_timestamp",
                    FunctionCall(
                        "_snuba_max_timestamp",
                        "max",
                        (Column("_snuba_gen_1", "ev", "_snuba_gen_1"),),
                    ),
                ),
            ],
            groupby=[Column("_snuba_project_id", "ev", "_snuba_project_id")],
            having=FunctionCall(
                None,
                "greater",
                (
                    FunctionCall(
                        None, "min", (Column("_snuba_gen_2", "ev", "_snuba_gen_2"),)
                    ),
                    Literal(None, "sometime"),
                ),
            ),
            granularity=123,
        ),
        id="Query with granularity, group by, aggregation and having clause",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=BASIC_JOIN,
            selected_columns=[
                SelectedExpression("a_col", Column("_snuba_a_col", "ev", "column")),
                SelectedExpression(
                    "a_func",
                    FunctionCall(
                        "_snuba_a_func",
                        "f",
                        (
                            Column("_snuba_a_col", "ev", "column"),
                            Column("_snuba_another_col", "gr", "another_column"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "another_func",
                    FunctionCall(
                        "_snuba_another_func",
                        "f",
                        (Column("_snuba_col3", "ev", "column3"),),
                    ),
                ),
            ],
            groupby=[
                FunctionCall(
                    "_snuba_another_func",
                    "f",
                    (Column("_snuba_col3", "ev", "column3"),),
                )
            ],
        ),
        CompositeQuery(
            from_clause=events_groups_join(
                events_node(
                    [
                        SelectedExpression(
                            "_snuba_a_col", Column("_snuba_a_col", None, "column")
                        ),
                        SelectedExpression(
                            "_snuba_another_func",
                            FunctionCall(
                                "_snuba_another_func",
                                "f",
                                (Column("_snuba_col3", None, "column3"),),
                            ),
                        ),
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                groups_node(
                    [
                        SelectedExpression(
                            "_snuba_another_col",
                            Column("_snuba_another_col", None, "another_column"),
                        ),
                        SelectedExpression(
                            "_snuba_id", Column("_snuba_id", None, "id")
                        ),
                    ]
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "a_col", Column("_snuba_a_col", "ev", "_snuba_a_col")
                ),
                SelectedExpression(
                    "a_func",
                    FunctionCall(
                        "_snuba_a_func",
                        "f",
                        (
                            Column("_snuba_a_col", "ev", "_snuba_a_col"),
                            Column("_snuba_another_col", "gr", "_snuba_another_col"),
                        ),
                    ),
                ),
                SelectedExpression(
                    "another_func",
                    Column("_snuba_another_func", "ev", "_snuba_another_func"),
                ),
            ],
            groupby=[Column("_snuba_another_func", "ev", "_snuba_another_func")],
        ),
        id="Identical expressions are pushed down only once [generated aliases are excluded]",
    ),
]


@pytest.mark.parametrize("original_query, processed_query", TEST_CASES)
def test_subquery_generator(
    original_query: CompositeQuery[Entity],
    processed_query: CompositeQuery[Entity],
) -> None:
    override_entity_map(EntityKey.EVENTS, Events())
    override_entity_map(EntityKey.GROUPEDMESSAGE, GroupedMessage())
    override_entity_map(EntityKey.GROUPASSIGNEE, GroupAssignee())

    generate_subqueries(original_query)

    original_map = cast(
        JoinClause[Entity], original_query.get_from_clause()
    ).get_alias_node_map()
    processed_map = cast(
        JoinClause[Entity], processed_query.get_from_clause()
    ).get_alias_node_map()

    for k, node in original_map.items():
        report = cast(LogicalQuery, node.data_source).equals(
            processed_map[k].data_source
        )
        assert report[0], f"Failed equality {k}: {report[1]}"

    report = original_query.equals(processed_query)
    assert report[0], f"Failed equality: {report[1]}"
    reset_entity_factory()


from_distributions = Entity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)


def metric_id_condition(metric_id: int, table_alias: str | None = None) -> FunctionCall:
    return FunctionCall(
        None,
        "equals",
        (
            Column("_snuba_metric_id", table_alias, "metric_id"),
            Literal(None, metric_id),
        ),
    )


def tag_column(tag: str, table_alias: str | None = None) -> SubscriptableReference:
    tag_val = mql_context.get("indexer_mappings").get(tag)  # type: ignore
    return SubscriptableReference(
        alias=f"_snuba_tags_raw[{tag_val}]",
        column=Column(
            alias="_snuba_tags_raw",
            table_name=table_alias,
            column_name="tags_raw",
        ),
        key=Literal(alias=None, value=f"{tag_val}"),
    )


def time_expression(table_alias: str | None = None) -> FunctionCall:
    alias_prefix = f"{table_alias}." if table_alias else ""
    return FunctionCall(
        f"_snuba_{alias_prefix}time",
        "toStartOfInterval",
        (
            Column("_snuba_timestamp", table_alias, "timestamp"),
            FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
            Literal(None, "Universal"),
        ),
    )


def condition(table_alias: str | None = None) -> list[FunctionCall]:
    conditions = [
        binary_condition(
            "greaterOrEquals",
            Column("_snuba_timestamp", table_alias, "timestamp"),
            Literal(None, datetime(2023, 11, 23, 18, 30)),
        ),
        binary_condition(
            "less",
            Column("_snuba_timestamp", table_alias, "timestamp"),
            Literal(None, datetime(2023, 11, 23, 22, 30)),
        ),
        binary_condition(
            "in",
            Column("_snuba_project_id", table_alias, "project_id"),
            literals_tuple(None, [Literal(alias=None, value=11)]),
        ),
        binary_condition(
            "in",
            Column("_snuba_org_id", table_alias, "org_id"),
            literals_tuple(None, [Literal(alias=None, value=1)]),
        ),
        binary_condition(
            "equals",
            Column("_snuba_use_case_id", table_alias, "use_case_id"),
            Literal(None, value="transactions"),
        ),
        binary_condition(
            "equals",
            Column("_snuba_granularity", table_alias, "granularity"),
            Literal(None, value=60),
        ),
    ]

    return conditions


# @pytest.mark.parametrize("original_query, processed_query", TEST_CASES)
def test_subquery_generator_metrics() -> None:
    #     original_query: CompositeQuery[Entity],
    #     processed_query: CompositeQuery[Entity],
    # ) -> None:
    override_entity_map(EntityKey.EVENTS, Events())
    override_entity_map(EntityKey.GROUPEDMESSAGE, GroupedMessage())
    override_entity_map(EntityKey.GROUPASSIGNEE, GroupAssignee())

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d3", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    original_query = CompositeQuery(
        from_clause=JoinClause(
            left_node=IndividualNode(
                alias="d3",
                data_source=from_distributions,
            ),
            right_node=IndividualNode(
                alias="d1",
                data_source=from_distributions,
            ),
            keys=[
                JoinCondition(
                    left=JoinConditionExpression(table_alias="d1", column="time"),
                    right=JoinConditionExpression(table_alias="d3", column="time"),
                )
            ],
            join_type=JoinType.INNER,
            join_modifier=None,
        ),
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d3"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
        ],
        groupby=[time_expression("d3"), time_expression("d1")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d1"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generate_subqueries(original_query)

    original_map = cast(
        JoinClause[Entity], original_query.get_from_clause()
    ).get_alias_node_map()
    processed_map = cast(
        JoinClause[Entity], processed_query.get_from_clause()
    ).get_alias_node_map()

    for k, node in original_map.items():
        report = cast(LogicalQuery, node.data_source).equals(
            processed_map[k].data_source
        )
        assert report[0], f"Failed equality {k}: {report[1]}"

    report = original_query.equals(processed_query)
    assert report[0], f"Failed equality: {report[1]}"
    reset_entity_factory()
