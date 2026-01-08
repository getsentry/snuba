from __future__ import annotations

from datetime import datetime

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import get_first_level_and_conditions
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, in_cond, literal
from snuba.query.expressions import Expression
from snuba.query.joins.metrics_subquery_generator import generate_metrics_subqueries
from snuba.query.logical import Query as LogicalQuery

entity_key = EntityKey.GENERIC_METRICS_DISTRIBUTIONS
distributions = Entity(entity_key, get_entity(entity_key).get_data_model())
tags_raw_d0 = NestedColumn("tags_raw", "d0")
tags_d0 = NestedColumn("tags", "d0")
tags_raw_d1 = NestedColumn("tags_raw", "d1")
tags_d1 = NestedColumn("tags", "d1")


def test_subquery_generator_metrics() -> None:
    from_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d0", column="d0.time"),
                right=JoinConditionExpression(table_alias="d1", column="d1.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )
    original_query = CompositeQuery(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                f.plus(
                    f.avg(column("value", "d0", "_snuba_value")),
                    f.avg(column("value", "d1", "_snuba_value")),
                    alias="_snuba_aggregate_value",
                ),
            ),
            SelectedExpression(
                "transaction",
                tags_d0["333333"],
            ),
            SelectedExpression(
                "transaction",
                tags_d1["333333"],
            ),
            SelectedExpression(
                "time",
                f.toStartOfInterval(
                    column("timestamp", "d1", "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_d1.time",
                ),
            ),
            SelectedExpression(
                "time",
                f.toStartOfInterval(
                    column("timestamp", "d0", "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_d0.time",
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            f.greaterOrEquals(
                column("timestamp", "d0", "_snuba_timestamp"),
                literal(datetime(2024, 4, 2, 9, 15)),
            ),
            f.less(
                column("timestamp", "d0", "_snuba_timestamp"),
                literal(datetime(2024, 4, 2, 15, 15)),
            ),
            in_cond(
                column("project_id", "d0", "_snuba_project_id"),
                f.tuple(literal(1), literal(2)),
            ),
            in_cond(column("org_id", "d0", "_snuba_org_id"), f.tuple(literal(101))),
            f.equals(
                column("use_case_id", "d0", "_snuba_use_case_id"),
                literal("performance"),
            ),
            f.equals(column("granularity", "d0", "_snuba_granularity"), literal(60)),
            f.equals(column("metric_id", "d0", "_snuba_metric_id"), literal(1068)),
            f.greaterOrEquals(
                column("timestamp", "d1", "_snuba_timestamp"),
                literal(datetime(2024, 4, 2, 9, 15)),
            ),
            f.less(
                column("timestamp", "d1", "_snuba_timestamp"),
                literal(datetime(2024, 4, 2, 15, 15)),
            ),
            in_cond(
                column("project_id", "d1", "_snuba_project_id"),
                f.tuple(literal(1), literal(2)),
            ),
            in_cond(column("org_id", "d1", "_snuba_org_id"), f.tuple(literal(101))),
            f.equals(
                column("use_case_id", "d1", "_snuba_use_case_id"),
                literal("performance"),
            ),
            f.equals(column("granularity", "d1", "_snuba_granularity"), literal(60)),
            f.equals(column("metric_id", "d1", "_snuba_metric_id"), literal(1068)),
        ),
        groupby=[
            tags_d0["333333"],
            tags_d1["333333"],
            f.toStartOfInterval(
                column("timestamp", "d1", "_snuba_timestamp"),
                f.toIntervalSecond(literal(60)),
                literal("Universal"),
                alias="_snuba_d1.time",
            ),
            f.toStartOfInterval(
                column("timestamp", "d0", "_snuba_timestamp"),
                f.toIntervalSecond(literal(60)),
                literal("Universal"),
                alias="_snuba_d0.time",
            ),
        ],
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                f.toStartOfInterval(
                    column("timestamp", "d0", "_snuba_timestamp"),
                    f.toIntervalSecond(literal(60)),
                    literal("Universal"),
                    alias="_snuba_d0.time",
                ),
            )
        ],
    )
    generate_metrics_subqueries(original_query)

    # Test outer selected columns
    expected_outer_query_selected = [
        SelectedExpression(
            "aggregate_value",
            f.plus(
                column("_snuba_gen_1", "d0", "_snuba_gen_1"),
                column("_snuba_gen_2", "d1", "_snuba_gen_2"),
                alias="_snuba_aggregate_value",
            ),
        ),
        SelectedExpression(
            "transaction", column("_snuba_tags[333333]", "d0", "_snuba_tags[333333]")
        ),
        SelectedExpression(
            "transaction", column("_snuba_tags[333333]", "d1", "_snuba_tags[333333]")
        ),
        SelectedExpression("time", column("_snuba_d1.time", "d1", "_snuba_d1.time")),
        SelectedExpression("time", column("_snuba_d0.time", "d0", "_snuba_d0.time")),
    ]

    selected_columns = original_query.get_selected_columns()
    assert selected_columns == expected_outer_query_selected

    # Test outer conditions
    assert (
        original_query.get_condition() is None
    ), "all conditions should be pushed down"

    # Test outer groupby
    expected_outer_query_groupby = []
    assert original_query.get_groupby() == expected_outer_query_groupby

    # Test orderby
    expected_outer_query_orderby = [
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=f.toStartOfInterval(
                column("timestamp", "d0", "_snuba_timestamp"),
                f.toIntervalSecond(literal(60)),
                literal("Universal"),
                alias="_snuba_d0.time",
            ),
        )
    ]
    assert original_query.get_orderby() == expected_outer_query_orderby

    # Test subqueries
    from_clause = original_query.get_from_clause()
    assert isinstance(from_clause, JoinClause)
    lhs_node = from_clause.left_node
    assert isinstance(lhs_node, IndividualNode)
    lhs = lhs_node.data_source
    assert isinstance(lhs, LogicalQuery)

    expected_lhs_selected = [
        SelectedExpression(
            "_snuba_d1.time",
            f.toStartOfInterval(
                column("timestamp", None, "_snuba_timestamp"),
                f.toIntervalSecond(literal(60)),
                literal("Universal"),
                alias="_snuba_d1.time",
            ),
        ),
        SelectedExpression(
            "_snuba_gen_2",
            f.avg(column("value", None, "_snuba_value"), alias="_snuba_gen_2"),
        ),
        SelectedExpression(
            "_snuba_tags[333333]",
            NestedColumn("tags")["333333"],
        ),
    ]

    selected_columns = lhs.get_selected_columns()
    assert selected_columns == expected_lhs_selected

    # The ordering of conditions is not guaranteed so we sort them by alias before asserting
    flattened_conditions: list[Expression] = get_first_level_and_conditions(
        lhs.get_condition()
    )
    flattened_conditions.sort(key=lambda x: x.alias)
    assert flattened_conditions == [
        f.greaterOrEquals(
            column("timestamp", None, "_snuba_timestamp"),
            literal(datetime(2024, 4, 2, 9, 15)),
            alias="_snuba_gen_10",
        ),
        f.less(
            column("timestamp", None, "_snuba_timestamp"),
            literal(datetime(2024, 4, 2, 15, 15)),
            alias="_snuba_gen_11",
        ),
        in_cond(
            column("project_id", None, "_snuba_project_id"),
            f.tuple(literal(1), literal(2)),
            "_snuba_gen_12",
        ),
        in_cond(
            column("org_id", None, "_snuba_org_id"),
            f.tuple(literal(101)),
            "_snuba_gen_13",
        ),
        f.equals(
            column("use_case_id", None, "_snuba_use_case_id"),
            literal("performance"),
            alias="_snuba_gen_14",
        ),
        f.equals(
            column("granularity", None, "_snuba_granularity"),
            literal(60),
            alias="_snuba_gen_15",
        ),
        f.equals(
            column("metric_id", None, "_snuba_metric_id"),
            literal(1068),
            alias="_snuba_gen_16",
        ),
    ]

    expected_lhs_query_groupby = [
        NestedColumn("tags")["333333"],
        f.toStartOfInterval(
            column("timestamp", None, "_snuba_timestamp"),
            f.toIntervalSecond(literal(60)),
            literal("Universal"),
            alias="_snuba_d1.time",
        ),
    ]
    assert lhs.get_groupby() == expected_lhs_query_groupby

    rhs = from_clause.right_node.data_source
    assert isinstance(rhs, LogicalQuery)

    expected_rhs_selected = [
        SelectedExpression(
            "_snuba_d0.time",
            f.toStartOfInterval(
                column("timestamp", None, "_snuba_timestamp"),
                f.toIntervalSecond(literal(60)),
                literal("Universal"),
                alias="_snuba_d0.time",
            ),
        ),
        SelectedExpression(
            "_snuba_gen_1",
            f.avg(column("value", None, "_snuba_value"), alias="_snuba_gen_1"),
        ),
        SelectedExpression(
            "_snuba_tags[333333]",
            NestedColumn("tags")["333333"],
        ),
    ]

    selected_columns = rhs.get_selected_columns()
    assert selected_columns == expected_rhs_selected

    # Test rhs conditions
    flattened_conditions: list[Expression] = get_first_level_and_conditions(
        rhs.get_condition()
    )
    flattened_conditions.sort(key=lambda x: x.alias)
    assert flattened_conditions == [
        f.greaterOrEquals(
            column("timestamp", None, "_snuba_timestamp"),
            literal(datetime(2024, 4, 2, 9, 15)),
            alias="_snuba_gen_3",
        ),
        f.less(
            column("timestamp", None, "_snuba_timestamp"),
            literal(datetime(2024, 4, 2, 15, 15)),
            alias="_snuba_gen_4",
        ),
        in_cond(
            column("project_id", None, "_snuba_project_id"),
            f.tuple(literal(1), literal(2)),
            "_snuba_gen_5",
        ),
        in_cond(
            column("org_id", None, "_snuba_org_id"),
            f.tuple(literal(101)),
            "_snuba_gen_6",
        ),
        f.equals(
            column("use_case_id", None, "_snuba_use_case_id"),
            literal("performance"),
            alias="_snuba_gen_7",
        ),
        f.equals(
            column("granularity", None, "_snuba_granularity"),
            literal(60),
            alias="_snuba_gen_8",
        ),
        f.equals(
            column("metric_id", None, "_snuba_metric_id"),
            literal(1068),
            alias="_snuba_gen_9",
        ),
    ]

    expected_rhs_query_groupby = [
        NestedColumn("tags")["333333"],
        f.toStartOfInterval(
            column("timestamp", None, "_snuba_timestamp"),
            f.toIntervalSecond(literal(60)),
            literal("Universal"),
            alias="_snuba_d0.time",
        ),
    ]
    assert rhs.get_groupby() == expected_rhs_query_groupby
