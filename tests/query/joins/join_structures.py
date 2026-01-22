from typing import Optional, Sequence, TypeVar

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity, SimpleDataSource, Table
from snuba.query.expressions import Expression
from snuba.query.logical import EntityQuery
from snuba.query.logical import Query as LogicalQuery
from tests.query.joins.equivalence_schema import (
    EVENTS_SCHEMA,
    GROUPS_SCHEMA,
)

TNode = TypeVar("TNode", bound=SimpleDataSource)


def build_node(
    alias: str,
    from_clause: Entity,
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression],
    granularity: Optional[int] = None,
) -> IndividualNode[Entity]:
    return IndividualNode(
        alias=alias,
        data_source=EntityQuery.from_query(
            LogicalQuery(  # type: ignore
                from_clause=from_clause,
                selected_columns=selected_columns,
                condition=condition,
                granularity=granularity,
            )
        ),
    )


def events_node(
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression] = None,
    granularity: Optional[int] = None,
) -> IndividualNode[Entity]:
    return build_node(
        "ev",
        Entity(EntityKey.EVENTS, ColumnSet(EVENTS_SCHEMA.columns)),
        selected_columns,
        condition,
        granularity,
    )


def groups_node(
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression] = None,
    granularity: Optional[int] = None,
) -> IndividualNode[Entity]:
    return build_node(
        "gr",
        Entity(EntityKey.PROFILES, ColumnSet(GROUPS_SCHEMA.columns)),
        selected_columns,
        condition,
        granularity,
    )


def build_clickhouse_node(
    alias: str,
    from_clause: Table,
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression],
    groupby: Optional[Sequence[Expression]] = None,
) -> IndividualNode[Table]:
    return IndividualNode(
        alias=alias,
        data_source=ClickhouseQuery(
            from_clause=from_clause,
            selected_columns=selected_columns,
            condition=condition,
            groupby=groupby,
        ),
    )


def clickhouse_events_node(
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression] = None,
    groupby: Optional[Sequence[Expression]] = None,
) -> IndividualNode[Table]:
    return build_clickhouse_node(
        "ev",
        Table("sentry_errors", EVENTS_SCHEMA, storage_key=StorageKey("dontmatter")),
        selected_columns,
        condition,
        groupby,
    )


def clickhouse_groups_node(
    selected_columns: Sequence[SelectedExpression],
    condition: Optional[Expression] = None,
) -> IndividualNode[Table]:
    return build_clickhouse_node(
        "gr",
        Table("profiles_local", GROUPS_SCHEMA, storage_key=StorageKey("dontmatter")),
        selected_columns,
        condition,
    )


def events_groups_join(
    left: IndividualNode[TNode],
    right: IndividualNode[TNode],
) -> JoinClause[TNode]:
    return JoinClause(
        left_node=left,
        right_node=right,
        keys=[
            JoinCondition(
                left=JoinConditionExpression("ev", "_snuba_group_id"),
                right=JoinConditionExpression("gr", "_snuba_id"),
            )
        ],
        join_type=JoinType.INNER,
    )
