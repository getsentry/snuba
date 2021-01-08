import pytest

from typing import Mapping, Optional, cast

from snuba.request.request_settings import HTTPRequestSettings
from snuba.query.joins.semi_joins import SemiJoinOptimizer
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity, SimpleDataSource, Table
from tests.query.joins.equivalence_schema import (
    GROUPS_ASSIGNEE,
    Events,
    GroupAssignee,
    GroupedMessage,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
    JoinModifier,
)
from tests.query.joins.join_structures import (
    build_clickhouse_node,
    events_groups_join,
    clickhouse_events_node,
    clickhouse_groups_node,
)


TEST_CASES = [
    pytest.param(
        CompositeQuery(
            from_clause=events_groups_join(
                clickhouse_events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                clickhouse_groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))],
                ),
            ),
            selected_columns=[],
        ),
        {"gr": JoinModifier.SEMI},
        id="Simple two table query with no reference. Semi join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=events_groups_join(
                clickhouse_events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                clickhouse_groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))]
                ),
            ),
            selected_columns=[
                SelectedExpression("group_id", Column("_snuba_col1", "gr", "_snuba_id"))
            ],
        ),
        {"gr": JoinModifier.SEMI},
        id="Query with reference to the join key. Semi join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=events_groups_join(
                clickhouse_events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                        SelectedExpression(
                            "_snuba_col1", Column("_snuba_col1", None, "something")
                        ),
                    ]
                ),
                clickhouse_groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))]
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "something", Column("_snuba_col1", "ev", "_snuba_col1")
                )
            ],
        ),
        {"gr": JoinModifier.SEMI},
        id="Query with reference to columns on the left side. Semi join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=events_groups_join(
                clickhouse_events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ]
                ),
                clickhouse_groups_node(
                    [
                        SelectedExpression(
                            "_snuba_id", Column("_snuba_id", None, "id")
                        ),
                        SelectedExpression(
                            "_snuba_col1", Column("_snuba_col1", None, "something")
                        ),
                    ]
                ),
            ),
            selected_columns=[
                SelectedExpression(
                    "group_id", Column("_snuba_col1", "gr", "_snuba_col1")
                )
            ],
        ),
        {"gr": None},
        id="Query with reference to columns on the right side. No semi join",
    ),
]


@pytest.mark.parametrize("query, expected_semi_join", TEST_CASES)
def test_subquery_generator(
    query: CompositeQuery[Table],
    expected_semi_join: Mapping[str, Optional[JoinModifier]],
) -> None:
    def assert_transformation(
        clause: JoinClause[Table], expected: Mapping[str, Optional[JoinModifier]]
    ) -> None:
        right_alias = clause.right_node.alias
        assert (
            right_alias in expected and clause.join_modifier == expected[right_alias]
        ), f"Invalid modifier for alias: {right_alias}, found: {clause.join_modifier}"

    SemiJoinOptimizer().process_query(query, HTTPRequestSettings())
    assert_transformation(
        cast(JoinClause[Table], query.get_from_clause()), expected_semi_join
    )
