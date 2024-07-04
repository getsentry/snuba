from __future__ import annotations

from datetime import datetime

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
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
from snuba.query.joins.metrics_subquery_generator import generate_metrics_subqueries
from snuba.query.logical import Query as LogicalQuery

entity_key = EntityKey.GENERIC_METRICS_DISTRIBUTIONS
distributions = Entity(entity_key, get_entity(entity_key).get_data_model())
tags_raw_d0 = NestedColumn("tags_raw", "d0")
tags_d0 = NestedColumn("tags", "d0")
tags_raw_d1 = NestedColumn("tags_raw", "d1")
tags_d1 = NestedColumn("tags", "d1")

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
            column("use_case_id", "d0", "_snuba_use_case_id"), literal("performance")
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
            column("use_case_id", "d1", "_snuba_use_case_id"), literal("performance")
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


def test_subquery_generator_metrics() -> None:
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
    expected_outer_query_groupby = [
        column("_snuba_tags[333333]", "d0", "_snuba_tags[333333]"),
        column("_snuba_tags[333333]", "d1", "_snuba_tags[333333]"),
        column("_snuba_d1.time", "d1", "_snuba_d1.time"),
        column("_snuba_d0.time", "d0", "_snuba_d0.time"),
    ]
    assert original_query.get_groupby() == expected_outer_query_groupby

    # Test orderby
    expected_outer_query_orderby = [
        OrderBy(
            direction=OrderByDirection.ASC,
            expression=column("_snuba_d0.time", "d0", "_snuba_d0.time"),
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

    # Test lhs conditions
    print("conds")
    print(lhs.get_condition())
    from snuba.query.conditions import ConditionFunctions, build_match

    print("try to find gran")
    match = build_match(
        col="granularity",
        ops=[ConditionFunctions.EQ],
        param_type=int,
    )
    found = match.match(lhs.get_condition())
    print(found)

    assert lhs.get_condition() == and_cond(
        and_cond(
            and_cond(
                f.equals(
                    column("granularity", None, "_snuba_granularity"),
                    literal(60),
                ),
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2024, 4, 2, 9, 15)),
                ),
            ),
            and_cond(
                and_cond(
                    in_cond(column("org_id"), f.tuple(literal(101))),
                    in_cond(column("org_id"), f.tuple(literal(101))),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1), literal(2)),
                    ),
                    f.equals(column("use_case_id"), literal("performance")),
                ),
            ),
        ),
        and_cond(
            and_cond(
                and_cond(
                    f.equals(column("use_case_id"), literal("performance")),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2024, 4, 2, 15, 15)),
                    ),
                ),
                and_cond(
                    f.equals(
                        column("granularity", None, "_snuba_granularity"),
                        literal(60),
                    ),
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(1068)
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2024, 4, 2, 15, 15)),
                    ),
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2024, 4, 2, 9, 15)),
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(1068)
                    ),
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1), literal(2)),
                    ),
                ),
            ),
        ),
    )

    # expected_lhs_query_groupby = [
    #     f.toStartOfInterval(
    #         column("timestamp", None, "_snuba_timestamp"),
    #         f.toIntervalSecond(literal(60)),
    #         literal("Universal"),
    #         alias="_snuba_d1.time",
    #     ),
    # ]
    # assert lhs.get_groupby() == expected_lhs_query_groupby

    # rhs = from_clause.right_node.data_source
    # assert isinstance(rhs, LogicalQuery)

    # expected_rhs_selected = [
    #     SelectedExpression(
    #         "_snuba_d0.time",
    #         f.toStartOfInterval(
    #             column("timestamp", None, "_snuba_timestamp"),
    #             f.toIntervalSecond(literal(60)),
    #             literal("Universal"),
    #             alias="_snuba_d0.time",
    #         ),
    #     ),
    #     SelectedExpression(
    #         "_snuba_gen_1",
    #         f.avg(column("value", None, "_snuba_value"), alias="_snuba_gen_1"),
    #     ),
    # ]

    # selected_columns = rhs.get_selected_columns()
    # assert selected_columns == expected_rhs_selected

    # # Test rhs conditions
    # assert rhs.get_condition() == and_cond(
    #     f.greaterOrEquals(
    #         column("timestamp", None, "_snuba_timestamp"),
    #         literal(datetime(2024, 4, 2, 9, 15)),
    #     ),
    #     f.less(
    #         column("timestamp", None, "_snuba_timestamp"),
    #         literal(datetime(2024, 4, 2, 15, 15)),
    #     ),
    #     in_cond(
    #         column("project_id", None, "_snuba_project_id"),
    #         f.tuple(literal(1), literal(2)),
    #     ),
    #     in_cond(column("org_id", None, "_snuba_org_id"), f.tuple(literal(101))),
    #     f.equals(
    #         column("use_case_id", None, "_snuba_use_case_id"), literal("performance")
    #     ),
    #     f.equals(column("granularity", None, "_snuba_granularity"), literal(60)),
    #     f.equals(column("metric_id", None, "_snuba_metric_id"), literal(1068)),
    #     f.greaterOrEquals(column("timestamp"), literal(datetime(2024, 4, 2, 9, 15))),
    #     f.less(column("timestamp"), literal(datetime(2024, 4, 2, 15, 15))),
    #     in_cond(column("project_id"), f.tuple(literal(1), literal(2))),
    #     in_cond(column("org_id"), f.tuple(literal(101))),
    #     f.equals(column("use_case_id"), literal("performance")),
    #     f.equals(column("granularity"), literal(60)),
    #     f.equals(column("metric_id"), literal(1068)),
    # )

    # expected_rhs_query_groupby = [
    #     f.toStartOfInterval(
    #         column("timestamp", None, "_snuba_timestamp"),
    #         f.toIntervalSecond(literal(60)),
    #         literal("Universal"),
    #         alias="_snuba_d0.time",
    #     ),
    # ]
    # assert rhs.get_groupby() == expected_rhs_query_groupby

    # print("SUCCESS")
    # assert False
