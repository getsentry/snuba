from typing import cast

import pytest
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import ENTITY_IMPL
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

BASIC_JOIN = JoinClause(
    left_node=IndividualNode(
        alias="ev", data_source=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, None),
    ),
    right_node=IndividualNode(
        alias="gr", data_source=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None),
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
        CompositeQuery(from_clause=BASIC_JOIN, selected_columns=[],),
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="ev",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "group_id"),
                            ),
                        ],
                    ),
                ),
                right_node=IndividualNode(
                    alias="gr",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_id", Column("_snuba_id", None, "id")
                            ),
                        ],
                    ),
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "_snuba_group_id"),
                        right=JoinConditionExpression("gr", "_snuba_id"),
                    )
                ],
                join_type=JoinType.INNER,
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
                        (Column("_snuba_event_id", "ev", "event_id"),),
                    ),
                ),
                SelectedExpression("group_id", Column("_snuba_group_id", "gr", "id")),
            ],
        ),
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="ev",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_event_id",
                                Column("_snuba_event_id", None, "event_id"),
                            ),
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "group_id"),
                            ),
                        ],
                    ),
                ),
                right_node=IndividualNode(
                    alias="gr",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA),
                        selected_columns=[
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
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "_snuba_group_id"),
                        right=JoinConditionExpression("gr", "_snuba_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "count_ev_id",
                    FunctionCall(
                        "_snuba_count_ev_id",
                        "count",
                        (Column(None, "ev", "_snuba_event_id"),),
                    ),
                ),
                SelectedExpression("group_id", Column(None, "gr", "_snuba_group_id")),
            ],
        ),
        id="Basic join with select",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="ev",
                    data_source=Entity(EntityKey.EVENTS, EVENTS_SCHEMA, None),
                ),
                right_node=IndividualNode(
                    alias="gr",
                    data_source=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA, None),
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "group_id"),
                        right=JoinConditionExpression("gr", "id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
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
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="ev",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_group_id",
                                Column("_snuba_group_id", None, "group_id"),
                            ),
                        ],
                    ),
                ),
                right_node=IndividualNode(
                    alias="gr",
                    data_source=LogicalQuery(
                        {},
                        from_clause=Entity(EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA),
                        selected_columns=[
                            SelectedExpression(
                                "_snuba_gen_1",
                                FunctionCall(
                                    "_snuba_gen_1",
                                    "equals",
                                    (
                                        Column("_snuba_group_id", None, "id"),
                                        Literal(None, 123),
                                    ),
                                ),
                            ),
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
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("ev", "_snuba_group_id"),
                        right=JoinConditionExpression("gr", "_snuba_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression("group_id", Column(None, "gr", "_snuba_group_id")),
            ],
            condition=Column(None, "gr", "_snuba_gen_1"),
        ),
        id="Query with condition",
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
                left_node=JoinClause(
                    left_node=IndividualNode(
                        alias="ev",
                        data_source=LogicalQuery(
                            {},
                            from_clause=Entity(EntityKey.EVENTS, EVENTS_SCHEMA),
                            selected_columns=[
                                SelectedExpression(
                                    "_snuba_group_id",
                                    Column("_snuba_group_id", None, "group_id"),
                                ),
                            ],
                        ),
                    ),
                    right_node=IndividualNode(
                        alias="gr",
                        data_source=LogicalQuery(
                            {},
                            from_clause=Entity(
                                EntityKey.GROUPEDMESSAGES, GROUPS_SCHEMA
                            ),
                            selected_columns=[
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
                    keys=[
                        JoinCondition(
                            left=JoinConditionExpression("ev", "_snuba_group_id"),
                            right=JoinConditionExpression("gr", "_snuba_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                ),
                right_node=IndividualNode(
                    alias="as",
                    data_source=LogicalQuery(
                        {},
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
                SelectedExpression("group_id", Column(None, "gr", "_snuba_group_id")),
            ],
        ),
        id="Multi entity join",
    ),
]


@pytest.mark.parametrize("original_query, processed_query", TEST_CASES)
def test_subquery_generator(
    original_query: CompositeQuery[Entity], processed_query: CompositeQuery[Entity],
) -> None:
    ENTITY_IMPL[EntityKey.EVENTS] = Events()
    ENTITY_IMPL[EntityKey.GROUPEDMESSAGES] = GroupedMessage()
    ENTITY_IMPL[EntityKey.GROUPASSIGNEE] = GroupAssignee()

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
    ENTITY_IMPL.clear()
