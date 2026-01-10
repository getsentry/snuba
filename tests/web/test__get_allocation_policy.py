from __future__ import annotations

from datetime import datetime
from typing import Union

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.configs.configuration import ResourceIdentifier
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.allocation_policies import AllocationPolicy, PassthroughPolicy
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
from snuba.web.db_query import _get_allocation_policies


class PermissiveJoinClause(JoinClause[Table]):
    def __post_init__(self) -> None:
        """JoinClause verifies that the join clause is references
        valid columns, these tests do not care about columns"""
        pass


events_table = Table(
    "errors",
    ColumnSet([]),
    allocation_policies=[PassthroughPolicy(ResourceIdentifier(StorageKey("flimflam")), [], {})],
    storage_key=StorageKey("errors"),
    final=False,
    sampling_rate=None,
    mandatory_conditions=[],
)

groups_table = Table(
    "groups",
    ColumnSet([]),
    allocation_policies=[PassthroughPolicy(ResourceIdentifier(StorageKey("jimjam")), [], {})],
    storage_key=StorageKey("groups"),
    final=False,
    sampling_rate=None,
    mandatory_conditions=[],
)

composite_query = CompositeQuery(
    from_clause=ClickhouseQuery(
        from_clause=events_table,
        selected_columns=[
            SelectedExpression("project_id", Column(None, None, "project_id")),
        ],
        groupby=[Column(None, None, "project_id")],
        condition=binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, "timestamp"),
            Literal(None, datetime(2020, 1, 1, 12, 0)),
        ),
    ),
    selected_columns=[
        SelectedExpression(
            "average",
            FunctionCall("average", "avg", (Column(None, None, "project_id"),)),
        ),
    ],
)


join_query = CompositeQuery(
    from_clause=PermissiveJoinClause(
        left_node=IndividualNode(alias="err", data_source=events_table),
        right_node=IndividualNode(
            alias="groups",
            data_source=groups_table,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression("err", "group_id"),
                right=JoinConditionExpression("groups", "id"),
            )
        ],
        join_type=JoinType.INNER,
    ),
    selected_columns=[],
)


@pytest.mark.parametrize(
    "query,expected_allocation_policies",
    [
        pytest.param(
            composite_query,
            [PassthroughPolicy(ResourceIdentifier(StorageKey("flimflam")), [], {})],
            id="composite query uses leaf query's allocation policy",
        ),
        pytest.param(
            CompositeQuery(
                from_clause=composite_query,
                selected_columns=[
                    SelectedExpression(
                        "average",
                        FunctionCall("average", "avg", (Column(None, None, "project_id"),)),
                    ),
                ],
            ),
            [PassthroughPolicy(ResourceIdentifier(StorageKey("flimflam")), [], {})],
            id="double nested composite query uses leaf query's allocation policy",
        ),
        pytest.param(
            join_query,
            [
                PassthroughPolicy(ResourceIdentifier(StorageKey("flimflam")), [], {}),
                PassthroughPolicy(ResourceIdentifier(StorageKey("jimjam")), [], {}),
            ],
            id="all allocation policies from joins are put together",
        ),
        pytest.param(
            ClickhouseQuery(
                from_clause=events_table,
                selected_columns=[
                    SelectedExpression("project_id", Column(None, None, "project_id")),
                ],
                groupby=[Column(None, None, "project_id")],
                condition=binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, None, "timestamp"),
                    Literal(None, datetime(2020, 1, 1, 12, 0)),
                ),
            ),
            [PassthroughPolicy(ResourceIdentifier(StorageKey("flimflam")), [], {})],
            id="simple query uses table's allocation policy",
        ),
    ],
)
def test__get_allocation_policies(
    query: Union[ClickhouseQuery, CompositeQuery[Table]],
    expected_allocation_policies: list[AllocationPolicy],
) -> None:
    assert _get_allocation_policies(query) == expected_allocation_policies
