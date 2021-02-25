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
events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
events_table_name = events_storage.get_table_writer().get_schema().get_table_name()

events_table = Table(
    events_table_name,
    events_storage.get_schema().get_columns(),
    final=False,
    sampling_rate=None,
    mandatory_conditions=events_storage.get_schema()
    .get_data_source()
    .get_mandatory_conditions(),
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
)

TEST_CASES = [
    pytest.param(
        CompositeQuery(
            from_clause=LogicalQuery(
                from_clause=events_ent,
                selected_columns=[
                    SelectedExpression("project_id", Column(None, None, "project_id")),
                    SelectedExpression(
                        "count_environment",
                        FunctionCall(
                            "count_environment",
                            "uniq",
                            (
                                SubscriptableReference(
                                    None,
                                    Column(None, None, "tags"),
                                    Literal(None, "environment"),
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
                        "average", "avg", (Column(None, None, "count_environment"),)
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
                            "count_environment",
                            FunctionCall(
                                "count_environment",
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
                                                Literal(None, "environment"),
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
                            "average", "avg", (Column(None, None, "count_environment"),)
                        ),
                    ),
                ],
            ),
            CompositeExecutionStrategy(get_cluster(StorageSetKey.EVENTS), [], {}, []),
            StorageSetKey.EVENTS,
            SubqueryProcessors(
                [],
                [*events_storage.get_query_processors(), MandatoryConditionApplier()],
            ),
            None,
        ),
        CompositeQuery(
            from_clause=ClickhouseQuery(
                from_clause=events_table,
                selected_columns=[
                    SelectedExpression("project_id", Column(None, None, "project_id")),
                    SelectedExpression(
                        "count_environment",
                        FunctionCall(
                            "count_environment",
                            function_name="ifNull",
                            parameters=(
                                FunctionCall(
                                    None,
                                    "uniq",
                                    (
                                        Column(
                                            alias=None,
                                            table_name=None,
                                            column_name="environment",
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
                        "average", "avg", (Column(None, None, "count_environment"),)
                    ),
                ),
            ],
        ),
        id="Query with a subquery",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(alias="err", data_source=events_ent,),
                right_node=IndividualNode(alias="groups", data_source=groups_ent,),
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
                    "f_release",
                    FunctionCall("f_release", "f", (Column(None, "err", "release"),),),
                ),
                SelectedExpression(
                    "_snuba_right", Column("_snuba_right", "groups", "right_col"),
                ),
            ],
            condition=binary_condition(
                ConditionFunctions.EQ,
                Column(None, "err", "project_id"),
                Literal(None, 1),
            ),
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
                                    "_snuba_group_id",
                                    Column("_snuba_group_id", None, "group_id"),
                                ),
                                SelectedExpression(
                                    "f_release",
                                    FunctionCall(
                                        "f_release",
                                        function_name="f",
                                        parameters=(
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
                                    "_snuba_id", Column("_snuba_id", None, "id")
                                ),
                                SelectedExpression(
                                    "_snuba_right",
                                    Column("_snuba_right", None, "right_col"),
                                ),
                            ],
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            left=JoinConditionExpression("err", "_snuba_group_id"),
                            right=JoinConditionExpression("groups", "_snuba_id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                ),
                selected_columns=[
                    SelectedExpression(
                        "f_release", Column("f_release", "err", "f_release"),
                    ),
                    SelectedExpression(
                        "_snuba_right",
                        Column("_snuba_right", "groups", "_snuba_right"),
                    ),
                ],
            ),
            CompositeExecutionStrategy(get_cluster(StorageSetKey.EVENTS), [], {}, []),
            StorageSetKey.EVENTS,
            None,
            {
                "err": SubqueryProcessors(
                    [],
                    [
                        *events_storage.get_query_processors(),
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
                                "_snuba_group_id",
                                FunctionCall(
                                    "_snuba_group_id",
                                    function_name="nullIf",
                                    parameters=(
                                        Column(None, None, "group_id"),
                                        Literal(None, 0),
                                    ),
                                ),
                            ),
                            SelectedExpression(
                                "f_release",
                                FunctionCall(
                                    "f_release",
                                    function_name="f",
                                    parameters=(
                                        Column(
                                            None,
                                            None,
                                            "sentry:release"
                                            if events_storage.get_storage_key()
                                            == StorageKey.EVENTS
                                            else "release",
                                        ),
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
                                "_snuba_id", Column("_snuba_id", None, "id")
                            ),
                            SelectedExpression(
                                "_snuba_right",
                                Column("_snuba_right", None, "right_col"),
                            ),
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
                        left=JoinConditionExpression("err", "_snuba_group_id"),
                        right=JoinConditionExpression("groups", "_snuba_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression(
                    "f_release", Column("f_release", "err", "f_release"),
                ),
                SelectedExpression(
                    "_snuba_right", Column("_snuba_right", "groups", "_snuba_right"),
                ),
            ],
        ),
        id="Simple join turned into a join of subqueries",
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
    # be different instances, thus making the simple equality fail.
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
