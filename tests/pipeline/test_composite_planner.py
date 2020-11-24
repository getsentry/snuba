import pytest
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import CompositeQueryPlan, SubqueryProcessors
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.composite import CompositeExecutionStrategy, CompositeQueryPlanner
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity, Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier
from snuba.request.request_settings import HTTPRequestSettings

events_ent = Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
events_table = Table(
    "sentry_local", get_storage(StorageKey.EVENTS).get_schema().get_columns()
)

groups_ent = Entity(
    EntityKey.GROUPEDMESSAGES, get_entity(EntityKey.GROUPEDMESSAGES).get_data_model()
)
groups_table = Table(
    "groupedmessage_local",
    get_storage(StorageKey.GROUPEDMESSAGES).get_schema().get_columns(),
)

TEST_CASES = [
    pytest.param(
        CompositeQuery(
            from_clause=LogicalQuery(
                {},
                from_clause=events_ent,
                selected_columns=[
                    SelectedExpression("project_id", Column(None, None, "project_id")),
                    SelectedExpression(
                        "count_release",
                        FunctionCall(
                            "count_release", "uniq", (Column(None, None, "release"),)
                        ),
                    ),
                ],
                groupby=[Column(None, None, "project_id")],
            ),
            selected_columns=[
                SelectedExpression(
                    "average",
                    FunctionCall(
                        "average", "avg", (Column(None, None, "count_release"),)
                    ),
                ),
            ],
        ),
        CompositeQueryPlan(
            CompositeQuery(
                from_clause=ClickhouseQuery(
                    from_clause=events_table,
                    selected_columns=[
                        SelectedExpression(
                            "project_id", Column(None, None, "project_id")
                        ),
                        SelectedExpression(
                            "count_release",
                            FunctionCall(
                                "count_release",
                                "uniq",
                                (
                                    build_mapping_expr(
                                        None,
                                        None,
                                        "tags",
                                        Literal(None, "sentry:release"),
                                    ),
                                ),
                            ),
                        ),
                    ],
                    groupby=[Column(None, None, "project_id")],
                ),
                selected_columns=[
                    SelectedExpression(
                        "average",
                        FunctionCall(
                            "average", "avg", (Column(None, None, "count_release"),)
                        ),
                    ),
                ],
            ),
            CompositeExecutionStrategy(),
            SubqueryProcessors(
                [],
                [
                    *get_storage(StorageKey.EVENTS).get_query_processors(),
                    MandatoryConditionApplier(),
                ],
            ),
            None,
        ),
        id="Query with a subquery",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="err",
                    data_source=LogicalQuery(
                        {},
                        from_clause=events_ent,
                        selected_columns=[
                            SelectedExpression(
                                "project_id", Column(None, None, "project_id")
                            ),
                            SelectedExpression(
                                "group_id", Column(None, None, "group_id")
                            ),
                            SelectedExpression(
                                "count_release",
                                FunctionCall(
                                    "count_release",
                                    "uniq",
                                    (Column(None, None, "release"),),
                                ),
                            ),
                        ],
                    ),
                ),
                right_node=IndividualNode(
                    alias="groups",
                    data_source=LogicalQuery(
                        {},
                        from_clause=groups_ent,
                        selected_columns=[
                            SelectedExpression(
                                "project_id", Column(None, None, "project_id")
                            ),
                            SelectedExpression("id", Column(None, None, "id")),
                        ],
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
                    "average",
                    FunctionCall(
                        "average", "avg", (Column(None, None, "count_release"),)
                    ),
                ),
            ],
        ),
        CompositeQueryPlan(
            CompositeQuery(
                from_clause=JoinClause(
                    left_node=IndividualNode(
                        alias="err",
                        data_source=LogicalQuery(
                            {},
                            from_clause=events_ent,
                            selected_columns=[
                                SelectedExpression(
                                    "project_id", Column(None, None, "project_id")
                                ),
                                SelectedExpression(
                                    "group_id", Column(None, None, "group_id")
                                ),
                                SelectedExpression(
                                    "count_release",
                                    FunctionCall(
                                        "count_release",
                                        "uniq",
                                        (Column(None, None, "release"),),
                                    ),
                                ),
                            ],
                        ),
                    ),
                    right_node=IndividualNode(
                        alias="groups",
                        data_source=LogicalQuery(
                            {},
                            from_clause=groups_ent,
                            selected_columns=[
                                SelectedExpression(
                                    "project_id", Column(None, None, "project_id")
                                ),
                                SelectedExpression("id", Column(None, None, "id")),
                            ],
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
                        "average",
                        FunctionCall(
                            "average",
                            "avg",
                            (
                                build_mapping_expr(
                                    None, None, "tags", Literal(None, "sentry:release"),
                                ),
                            ),
                        ),
                    ),
                ],
            ),
            CompositeExecutionStrategy(),
            None,
            {
                "err": SubqueryProcessors(
                    [],
                    [
                        *get_storage(StorageKey.EVENTS).get_query_processors(),
                        MandatoryConditionApplier(),
                    ],
                ),
                "groups": SubqueryProcessors(
                    [],
                    [
                        *get_storage(StorageKey.GROUPEDMESSAGES).get_query_processors(),
                        MandatoryConditionApplier(),
                    ],
                ),
            },
        ),
        id="Join of two subqueries",
    ),
]


@pytest.mark.parametrize("logical_query, composite_plan", TEST_CASES)
def test_composite_planner(
    logical_query: CompositeQuery[Entity], composite_plan: CompositeQueryPlan
) -> None:
    def assert_subquery_processors_equality(
        query: SubqueryProcessors, expected: SubqueryProcessors
    ) -> None:
        assert [type(x) for x in query.plan_processors] == [
            type(x) for x in expected.plan_processors
        ]
        assert [type(x) for x in query.db_processors] == [
            type(x) for x in expected.db_processors
        ]

    plan = CompositeQueryPlanner(logical_query, HTTPRequestSettings()).execute()
    assert plan.query.equals(composite_plan.query)

    # We cannot simply check the equality between the plans because
    # we need to verify processors are of the same type, they can
    # be different innstances, thus making the simple equality fail.
    query_processors = plan.root_processors is not None
    expected_processors = composite_plan.root_processors is not None
    assert query_processors == expected_processors

    if plan.root_processors is not None and composite_plan.root_processors is not None:
        assert_subquery_processors_equality(
            plan.root_processors, composite_plan.root_processors,
        )

    query_alias_processors = plan.aliased_processors is not None
    expected_alias_processors = composite_plan.aliased_processors is not None
    assert query_alias_processors == expected_alias_processors

    if (
        plan.aliased_processors is not None
        and composite_plan.aliased_processors is not None
    ):
        assert len(plan.aliased_processors) == len(composite_plan.aliased_processors)
        for k in plan.aliased_processors:
            assert_subquery_processors_equality(
                plan.aliased_processors[k], composite_plan.aliased_processors[k],
            )
