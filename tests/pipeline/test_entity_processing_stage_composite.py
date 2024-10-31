import uuid
from datetime import datetime
from typing import cast

import pytest

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.pipeline.query_pipeline import QueryPipelineResult
from snuba.pipeline.stages.query_processing import EntityProcessingStage
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
from snuba.query.data_source.simple import Entity, Table
from snuba.query.exceptions import ValidationException
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics.timer import Timer

events_ent = Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model())
events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
assert events_storage is not None
events_table_name = events_storage.get_table_writer().get_schema().get_table_name()

events_table = Table(
    events_table_name,
    events_storage.get_schema().get_columns(),
    storage_key=events_storage.get_storage_key(),
    allocation_policies=events_storage.get_allocation_policies(),
    final=False,
    sampling_rate=None,
    mandatory_conditions=events_storage.get_schema()
    .get_data_source()
    .get_mandatory_conditions(),
)

groups_ent = Entity(
    EntityKey.GROUPEDMESSAGE, get_entity(EntityKey.GROUPEDMESSAGE).get_data_model()
)
groups_storage = get_storage(StorageKey.GROUPEDMESSAGES)
groups_schema = groups_storage.get_schema()
assert isinstance(groups_schema, TableSchema)

groups_table = Table(
    groups_schema.get_table_name(),
    groups_schema.get_columns(),
    storage_key=groups_storage.get_storage_key(),
    allocation_policies=groups_storage.get_allocation_policies(),
    final=False,
    sampling_rate=None,
    mandatory_conditions=groups_schema.get_data_source().get_mandatory_conditions(),
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
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "project_id"),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        ConditionFunctions.GTE,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 1, 1, 12, 0)),
                    ),
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
                                        build_mapping_expr(
                                            None,
                                            None,
                                            "tags",
                                            Literal(None, "environment"),
                                            "value",
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
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "project_id"),
                        Literal(None, 1),
                    ),
                    binary_condition(
                        ConditionFunctions.GTE,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 1, 1, 12, 0)),
                    ),
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
        id="Subquery",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=CompositeQuery(
                from_clause=LogicalQuery(
                    from_clause=events_ent,
                    selected_columns=[
                        SelectedExpression(
                            "project_id", Column(None, None, "project_id")
                        ),
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
                        BooleanFunctions.AND,
                        binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "project_id"),
                            Literal(None, 1),
                        ),
                        binary_condition(
                            ConditionFunctions.GTE,
                            Column(None, None, "timestamp"),
                            Literal(None, datetime(2020, 1, 1, 12, 0)),
                        ),
                    ),
                ),
                selected_columns=[
                    SelectedExpression(
                        "max",
                        FunctionCall(
                            "max", "max", (Column(None, None, "count_environment"),)
                        ),
                    ),
                ],
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
        CompositeQuery(
            from_clause=CompositeQuery(
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
                                                "value",
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
                        BooleanFunctions.AND,
                        binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "project_id"),
                            Literal(None, 1),
                        ),
                        binary_condition(
                            ConditionFunctions.GTE,
                            Column(None, None, "timestamp"),
                            Literal(None, datetime(2020, 1, 1, 12, 0)),
                        ),
                    ),
                ),
                selected_columns=[
                    SelectedExpression(
                        "max",
                        FunctionCall(
                            "max", "max", (Column(None, None, "count_environment"),)
                        ),
                    ),
                ],
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
        id="Nested subquery",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="err",
                    data_source=events_ent,
                ),
                right_node=IndividualNode(
                    alias="groups",
                    data_source=groups_ent,
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
                    "f_release",
                    FunctionCall(
                        "f_release",
                        "f",
                        (Column(None, "err", "release"),),
                    ),
                ),
                SelectedExpression(
                    "_snuba_right",
                    Column("_snuba_right", "groups", "status"),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "err", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, "err", "timestamp"),
                    Literal(None, datetime(2020, 1, 1, 12, 0)),
                ),
            ),
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
                                            "value",
                                        ),
                                    ),
                                ),
                            ),
                        ],
                        condition=binary_condition(
                            BooleanFunctions.AND,
                            binary_condition(
                                ConditionFunctions.EQ,
                                Column(None, None, "project_id"),
                                Literal(None, 1),
                            ),
                            binary_condition(
                                ConditionFunctions.GTE,
                                Column(None, None, "timestamp"),
                                Literal(None, datetime(2020, 1, 1, 12, 0)),
                            ),
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
                                Column("_snuba_right", None, "status"),
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
                    "f_release",
                    Column("f_release", "err", "f_release"),
                ),
                SelectedExpression(
                    "_snuba_right",
                    Column("_snuba_right", "groups", "_snuba_right"),
                ),
            ],
        ),
        id="Join query",
    ),
]


@pytest.mark.parametrize("logical_query, expected", TEST_CASES)
@pytest.mark.clickhouse_db
def test_composite(
    logical_query: CompositeQuery[Entity],
    expected: CompositeQuery[Table],
) -> None:
    request = Request(
        id=uuid.uuid4(),
        original_body={"query": "placeholder"},
        query=cast(LogicalQuery, logical_query),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    actual = (
        EntityProcessingStage()
        .execute(
            QueryPipelineResult(
                data=request,
                query_settings=request.query_settings,
                timer=Timer("test"),
                error=None,
            )
        )
        .data
    )
    assert actual == expected


TEST_CASES_INVALID = [
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=IndividualNode(
                    alias="err",
                    data_source=events_ent,
                ),
                right_node=IndividualNode(
                    alias="groups",
                    data_source=groups_ent,
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
                    "f_release",
                    FunctionCall(
                        "f_release",
                        "f",
                        (Column(None, "err", "release"),),
                    ),
                ),
                SelectedExpression(
                    "_snuba_right",
                    Column("_snuba_right", "groups", "status"),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, "err", "project_id"),
                    Literal(None, 1),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.GTE,
                        Column(None, "err", "timestamp"),
                        Literal(None, datetime(2020, 1, 1, 12, 0)),
                    ),
                    binary_condition(
                        ConditionFunctions.GTE,
                        Column(None, "err", "foo"),
                        Literal(None, 1),
                    ),
                ),
            ),
        ),
        ValidationException,
        id="Join query with missing column",
    ),
]


@pytest.mark.parametrize("logical_query, expected_error", TEST_CASES_INVALID)
@pytest.mark.clickhouse_db
def test_invalid_composite(
    logical_query: CompositeQuery[Entity],
    expected_error: Exception,
) -> None:
    request = Request(
        id=uuid.uuid4(),
        original_body={"query": "placeholder"},
        query=cast(LogicalQuery, logical_query),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            get_app_id("blah"), {"tenant_type": "tenant_id"}, "blah", None, None, None
        ),
    )
    actual = EntityProcessingStage().execute(
        QueryPipelineResult(
            data=request,
            query_settings=request.query_settings,
            timer=Timer("test"),
            error=None,
        )
    )
    assert actual.error and not actual.data
    assert isinstance(type(actual.error), type(expected_error))
