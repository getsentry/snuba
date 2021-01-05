from copy import deepcopy
from typing import Union

import pytest
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.plans.query_plan import CompositeQueryPlan, SubqueryProcessors
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.composite import (
    CompositeExecutionPipeline,
    CompositeExecutionStrategy,
    CompositeQueryPlanner,
)
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
from snuba.query.data_source.simple import Entity, Table
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mandatory_condition_applier import MandatoryConditionApplier
from snuba.reader import Reader
from snuba.request.request_settings import HTTPRequestSettings, RequestSettings
from snuba.web import QueryResult

events_ent = Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
events_storage = get_storage(StorageKey.EVENTS)
events_table = Table(
    "sentry_local",
    events_storage.get_schema().get_columns(),
    final=False,
    sampling_rate=None,
    mandatory_conditions=events_storage.get_schema()
    .get_data_source()
    .get_mandatory_conditions(),
    prewhere_candidates=events_storage.get_schema()
    .get_data_source()
    .get_prewhere_candidates(),
)

groups_ent = Entity(
    EntityKey.GROUPEDMESSAGES, get_entity(EntityKey.GROUPEDMESSAGES).get_data_model()
)
groups_storage = get_storage(StorageKey.GROUPEDMESSAGES)
groups_table = Table(
    "groupedmessage_local",
    groups_storage.get_schema().get_columns(),
    final=False,
    sampling_rate=None,
    mandatory_conditions=groups_storage.get_schema()
    .get_data_source()
    .get_mandatory_conditions(),
    prewhere_candidates=groups_storage.get_schema()
    .get_data_source()
    .get_prewhere_candidates(),
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
                            "count_release",
                            "uniq",
                            (
                                SubscriptableReference(
                                    None,
                                    Column(None, None, "tags"),
                                    Literal(None, "sentry:release"),
                                ),
                            ),
                        ),
                    ),
                ],
                groupby=[Column(None, None, "project_id")],
                condition=binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "project_id"),
                    Literal(None, 1),
                ),
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
                                function_name="ifNull",
                                parameters=(
                                    FunctionCall(
                                        None,
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
                                    Literal(alias=None, value=0),
                                ),
                            ),
                        ),
                    ],
                    groupby=[Column(None, None, "project_id")],
                    condition=binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "project_id"),
                        Literal(None, 1),
                    ),
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
            CompositeExecutionStrategy(get_cluster(StorageSetKey.EVENTS), [], {}),
            StorageSetKey.EVENTS,
            SubqueryProcessors(
                [],
                [
                    *get_storage(StorageKey.EVENTS).get_query_processors(),
                    MandatoryConditionApplier(),
                ],
            ),
            None,
        ),
        CompositeQuery(
            from_clause=ClickhouseQuery(
                from_clause=events_table,
                selected_columns=[
                    SelectedExpression("project_id", Column(None, None, "project_id")),
                    SelectedExpression(
                        "count_release",
                        FunctionCall(
                            "count_release",
                            function_name="ifNull",
                            parameters=(
                                FunctionCall(
                                    None,
                                    "uniq",
                                    (
                                        Column(
                                            alias=None,
                                            table_name=None,
                                            column_name="sentry:release",
                                        ),
                                    ),
                                ),
                                Literal(alias=None, value=0),
                            ),
                        ),
                    ),
                ],
                condition=binary_condition(
                    ConditionFunctions.EQ,
                    Column(alias=None, table_name=None, column_name="deleted"),
                    Literal(alias=None, value=0),
                ),
                groupby=[Column(None, None, "project_id")],
                prewhere=binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "project_id"),
                    Literal(None, 1),
                ),
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
                        condition=binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "project_id"),
                            Literal(None, 1),
                        ),
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
                        data_source=ClickhouseQuery(
                            from_clause=events_table,
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
                                        function_name="ifNull",
                                        parameters=(
                                            FunctionCall(
                                                None,
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
                                            Literal(alias=None, value=0),
                                        ),
                                    ),
                                ),
                            ],
                            condition=binary_condition(
                                ConditionFunctions.EQ,
                                Column(None, None, "project_id"),
                                Literal(None, 1),
                            ),
                        ),
                    ),
                    right_node=IndividualNode(
                        alias="groups",
                        data_source=ClickhouseQuery(
                            from_clause=groups_table,
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
            CompositeExecutionStrategy(get_cluster(StorageSetKey.EVENTS), [], {}),
            StorageSetKey.EVENTS,
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
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="err",
                    data_source=ClickhouseQuery(
                        from_clause=events_table,
                        selected_columns=[
                            SelectedExpression(
                                "project_id", Column(None, None, "project_id")
                            ),
                            SelectedExpression(
                                "group_id",
                                FunctionCall(
                                    None,
                                    function_name="nullIf",
                                    parameters=(
                                        Column(None, None, "group_id"),
                                        Literal(None, 0),
                                    ),
                                ),
                            ),
                            SelectedExpression(
                                "count_release",
                                FunctionCall(
                                    "count_release",
                                    function_name="ifNull",
                                    parameters=(
                                        FunctionCall(
                                            None,
                                            "uniq",
                                            (Column(None, None, "sentry:release"),),
                                        ),
                                        Literal(None, 0),
                                    ),
                                ),
                            ),
                        ],
                        condition=binary_condition(
                            ConditionFunctions.EQ,
                            Column(alias=None, table_name=None, column_name="deleted"),
                            Literal(alias=None, value=0),
                        ),
                        prewhere=binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "project_id"),
                            Literal(None, 1),
                        ),
                    ),
                ),
                right_node=IndividualNode(
                    alias="groups",
                    data_source=ClickhouseQuery(
                        from_clause=groups_table,
                        selected_columns=[
                            SelectedExpression(
                                "project_id", Column(None, None, "project_id")
                            ),
                            SelectedExpression("id", Column(None, None, "id")),
                        ],
                        condition=binary_condition(
                            ConditionFunctions.EQ,
                            Column(
                                alias=None,
                                table_name=None,
                                column_name="record_deleted",
                            ),
                            Literal(alias=None, value=0),
                        ),
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
        id="Join of two subqueries",
    ),
]


@pytest.mark.parametrize("logical_query, composite_plan, processed_query", TEST_CASES)
def test_composite_planner(
    logical_query: CompositeQuery[Entity],
    composite_plan: CompositeQueryPlan,
    processed_query: CompositeQuery[Table],
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

    plan = CompositeQueryPlanner(
        deepcopy(logical_query), HTTPRequestSettings()
    ).build_best_plan()
    report = plan.query.equals(composite_plan.query)
    assert report[0], f"Mismatch: {report[1]}"

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

    def runner(
        query: Union[ClickhouseQuery, CompositeQuery[Table]],
        request_settings: RequestSettings,
        reader: Reader,
    ) -> QueryResult:
        report = query.equals(processed_query)
        assert report[0], f"Mismatch: {report[1]}"
        return QueryResult({"data": []}, {},)

    CompositeExecutionPipeline(logical_query, HTTPRequestSettings(), runner).execute()
