from __future__ import annotations

from typing import cast

import pytest

from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import override_entity_map, reset_entity_factory
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.joins.subquery_generator import generate_subqueries
from snuba.query.logical import Query as LogicalQuery
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


import re
from datetime import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import binary_condition, combine_and_conditions
from snuba.query.dsl import arrayElement, divide, multiply, plus, literals_tuple
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.composite import CompositeQuery
from snuba.query.mql.parser import parse_mql_query
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
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
