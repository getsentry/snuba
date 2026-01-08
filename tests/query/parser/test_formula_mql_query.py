from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition, combine_and_conditions
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import arrayElement, divide, literals_tuple, minus, plus
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.mql.parser import parse_mql_query

# Commonly used expressions
from_distributions = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)


def time_expression(
    table_alias: str | None = None, to_interval_seconds: int | None = 60
) -> FunctionCall:
    alias_prefix = f"{table_alias}." if table_alias else ""
    return FunctionCall(
        f"_snuba_{alias_prefix}time",
        "toStartOfInterval",
        (
            Column("_snuba_timestamp", table_alias, "timestamp"),
            FunctionCall(None, "toIntervalSecond", (Literal(None, to_interval_seconds),)),
            Literal(None, "Universal"),
        ),
    )


def subscriptable_expression(
    tag_key: str, table_alias: str | None = None
) -> SubscriptableReference:
    return SubscriptableReference(
        alias=f"_snuba_tags_raw[{tag_key}]",
        column=Column(alias="_snuba_tags_raw", table_name=table_alias, column_name="tags_raw"),
        key=Literal(alias=None, value=tag_key),
    )


def condition(table_alias: str | None = None) -> list[FunctionCall]:
    conditions = [
        binary_condition(
            "greaterOrEquals",
            Column("_snuba_timestamp", table_alias, "timestamp"),
            Literal(None, datetime(2023, 11, 23, 18, 30)),
        ),
        binary_condition(
            "less",
            Column("_snuba_timestamp", table_alias, "timestamp"),
            Literal(None, datetime(2023, 11, 23, 22, 30)),
        ),
        binary_condition(
            "in",
            Column("_snuba_project_id", table_alias, "project_id"),
            literals_tuple(None, [Literal(alias=None, value=11)]),
        ),
        binary_condition(
            "in",
            Column("_snuba_org_id", table_alias, "org_id"),
            literals_tuple(None, [Literal(alias=None, value=1)]),
        ),
        binary_condition(
            "equals",
            Column("_snuba_use_case_id", table_alias, "use_case_id"),
            Literal(None, value="transactions"),
        ),
        binary_condition(
            "equals",
            Column("_snuba_granularity", table_alias, "granularity"),
            Literal(None, value=60),
        ),
    ]

    return conditions


mql_context = {
    "entity": "generic_metrics_distributions",
    "start": "2023-11-23T18:30:00",
    "end": "2023-11-23T22:30:00",
    "rollup": {
        "granularity": 60,
        "interval": 60,
        "with_totals": "False",
        "orderby": None,
    },
    "scope": {
        "org_ids": [1],
        "project_ids": [11],
        "use_case_id": "transactions",
    },
    "indexer_mappings": {
        "d:transactions/duration@millisecond": 123456,
        "d:transactions/measurements.fp@millisecond": 789012,
        "status_code": 222222,
        "transaction": 333333,
    },
    "limit": None,
    "offset": None,
}


def timeseries(agg: str, metric_id: int, condition: FunctionCall | None = None) -> FunctionCall:
    metric_condition = FunctionCall(
        None,
        "equals",
        (
            Column(
                "_snuba_metric_id",
                None,
                "metric_id",
            ),
            Literal(None, metric_id),
        ),
    )
    if condition:
        metric_condition = FunctionCall(
            None,
            "and",
            (
                condition,
                metric_condition,
            ),
        )

    return FunctionCall(
        None,
        agg,
        (
            Column("_snuba_value", None, "value"),
            metric_condition,
        ),
    )


def metric_id_condition(metric_id: int, table_alias: str | None = None) -> FunctionCall:
    return FunctionCall(
        None,
        "equals",
        (
            Column("_snuba_metric_id", table_alias, "metric_id"),
            Literal(None, metric_id),
        ),
    )


def tag_column(tag: str, table_alias: str | None = None) -> SubscriptableReference:
    tag_val = mql_context.get("indexer_mappings").get(tag)  # type: ignore
    return SubscriptableReference(
        alias=f"_snuba_tags_raw[{tag_val}]",
        column=Column(
            alias="_snuba_tags_raw",
            table_name=table_alias,
            column_name="tags_raw",
        ),
        key=Literal(alias=None, value=f"{tag_val}"),
    )


def test_simple_formula() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_bracket_on_formula() -> None:
    query_body = "(min(`d:transactions/duration@millisecond`){status_code:400} - max(`d:transactions/duration@millisecond`){status_code:404}) / (sum(`d:transactions/duration@millisecond`){status_code:200} + avg(`d:transactions/duration@millisecond`){status_code:418})"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            minus(
                FunctionCall(
                    None,
                    "min",
                    (Column("_snuba_value", "d0", "value"),),
                ),
                FunctionCall(
                    None,
                    "max",
                    (Column("_snuba_value", "d1", "value"),),
                ),
            ),
            plus(
                FunctionCall(
                    None,
                    "sum",
                    (Column("_snuba_value", "d2", "value"),),
                ),
                FunctionCall(
                    None,
                    "avg",
                    (Column("_snuba_value", "d3", "value"),),
                ),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=JoinClause(
            left_node=JoinClause(
                left_node=IndividualNode(
                    alias="d3",
                    data_source=from_distributions,
                ),
                right_node=IndividualNode(
                    alias="d2",
                    data_source=from_distributions,
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression(table_alias="d3", column="d3.time"),
                        right=JoinConditionExpression(table_alias="d2", column="d2.time"),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=None,
            ),
            right_node=IndividualNode(
                alias="d1",
                data_source=from_distributions,
            ),
            keys=[
                JoinCondition(
                    left=JoinConditionExpression(table_alias="d2", column="d2.time"),
                    right=JoinConditionExpression(table_alias="d1", column="d1.time"),
                )
            ],
            join_type=JoinType.INNER,
            join_modifier=None,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            ),
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "400")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d1"), Literal(None, "404")
    )
    tag_condition3 = binary_condition(
        "equals", tag_column("status_code", "d2"), Literal(None, "200")
    )
    tag_condition4 = binary_condition(
        "equals", tag_column("status_code", "d3"), Literal(None, "418")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    metric_condition3 = metric_id_condition(123456, "d2")
    metric_condition4 = metric_id_condition(123456, "d3")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + condition("d2")
        + condition("d3")
        + [
            tag_condition1,
            metric_condition1,
            tag_condition2,
            metric_condition2,
            tag_condition3,
            metric_condition3,
            tag_condition4,
            metric_condition4,
        ]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d3"),
            ),
            SelectedExpression(
                "time",
                time_expression("d2"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[
            time_expression("d3"),
            time_expression("d2"),
            time_expression("d1"),
            time_expression("d0"),
        ],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_multiple_filter_same_groupby_formula() -> None:
    query_body = 'avg(`d:transactions/duration@millisecond`){transaction:"test" and status_code:418} by (status_code) / min(`d:transactions/duration@millisecond`){transaction:"prod" or status_code:400} by (status_code)'

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "avg",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "min",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="tags_raw[222222]"),
                right=JoinConditionExpression(table_alias="d0", column="tags_raw[222222]"),
            ),
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            ),
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("transaction", "d0"), Literal(None, "test")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "418")
    )
    tag_condition3 = binary_condition(
        "or",
        binary_condition("equals", tag_column("transaction", "d1"), Literal(None, "prod")),
        binary_condition("equals", tag_column("status_code", "d1"), Literal(None, "400")),
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + [
            tag_condition1,
            tag_condition2,
            metric_condition1,
            tag_condition3,
            metric_condition2,
        ]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "status_code",
                subscriptable_expression("222222", "d0"),
            ),
            SelectedExpression(
                "status_code",
                subscriptable_expression("222222", "d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[
            subscriptable_expression("222222", "d0"),
            subscriptable_expression("222222", "d1"),
            time_expression("d1"),
            time_expression("d0"),
        ],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)

    assert eq, reason


def test_distribute_tags() -> None:
    query_body = "max(`d:transactions/duration@millisecond`){status_code:404} / (sum(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)){status_code:418}"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "max",
                (Column("_snuba_value", "d0", "value"),),
            ),
            plus(
                FunctionCall(
                    None,
                    "sum",
                    (Column("_snuba_value", "d1", "value"),),
                ),
                FunctionCall(
                    None,
                    "avg",
                    (Column("_snuba_value", "d2", "value"),),
                ),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=JoinClause(
            left_node=IndividualNode(
                alias="d2",
                data_source=from_distributions,
            ),
            right_node=IndividualNode(
                alias="d1",
                data_source=from_distributions,
            ),
            keys=[
                JoinCondition(
                    left=JoinConditionExpression(table_alias="d2", column="d2.time"),
                    right=JoinConditionExpression(table_alias="d1", column="d1.time"),
                )
            ],
            join_type=JoinType.INNER,
            join_modifier=None,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            ),
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "404")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d1"), Literal(None, "418")
    )
    tag_condition3 = binary_condition(
        "equals", tag_column("status_code", "d2"), Literal(None, "418")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    metric_condition3 = metric_id_condition(123456, "d2")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + condition("d2")
        + [
            tag_condition1,
            metric_condition1,
            tag_condition2,
            metric_condition2,
            tag_condition3,
            metric_condition3,
        ]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d2"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d2"), time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_with_scalar() -> None:
    query_body = "(-1 - avg(`d:transactions/duration@millisecond`){status_code:418}) / sum(`d:transactions/duration@millisecond`){status_code:418} + 1"

    expected_selected = SelectedExpression(
        "aggregate_value",
        plus(
            divide(
                minus(
                    Literal(None, -1.0),
                    FunctionCall(
                        None,
                        "avg",
                        (Column("_snuba_value", "d0", "value"),),
                    ),
                ),
                FunctionCall(
                    None,
                    "sum",
                    (Column("_snuba_value", "d1", "value"),),
                ),
            ),
            Literal(None, 1.0),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "418")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d1"), Literal(None, "418")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + [tag_condition1, metric_condition1, tag_condition2, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="tags_raw[333333]"),
                right=JoinConditionExpression(table_alias="d0", column="tags_raw[333333]"),
            ),
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            ),
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d0"),
            ),
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[
            subscriptable_expression("333333", "d0"),
            subscriptable_expression("333333", "d1"),
            time_expression("d1"),
            time_expression("d0"),
        ],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_groupby_with_totals() -> None:
    mql_context_new = deepcopy(mql_context)
    mql_context_new["rollup"]["with_totals"] = "True"
    mql_context_new["rollup"]["interval"] = None
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="tags_raw[333333]"),
                right=JoinConditionExpression(table_alias="d0", column="tags_raw[333333]"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d0"),
            ),
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d1"),
            ),
        ],
        groupby=[
            subscriptable_expression("333333", "d0"),
            subscriptable_expression("333333", "d1"),
        ],
        condition=formula_condition,
        limit=1000,
        offset=0,
        totals=True,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context_new, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_mismatch_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by status_code"
    generic_metrics = get_dataset(
        "generic_metrics",
    )
    with pytest.raises(
        Exception,
        match=re.escape("All terms in a formula must have the same groupby"),
    ):
        parse_mql_query(str(query_body), mql_context, generic_metrics)


def test_onesided_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`)"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            ),
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d0"),
            ),
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[
            subscriptable_expression("333333", "d0"),
            time_expression("d1"),
            time_expression("d0"),
        ],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_with_nested_functions() -> None:
    query_body = "apdex(avg(`d:transactions/duration@millisecond`){status_code:418}, 123) / max(`d:transactions/duration@millisecond`){status_code:400}"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "apdex",
                (
                    FunctionCall(None, "avg", (Column("_snuba_value", "d0", "value"),)),
                    Literal(None, 123.0),
                ),
            ),
            FunctionCall(
                None,
                "max",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "418")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d1"), Literal(None, "400")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + [tag_condition1, metric_condition1, tag_condition2, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_with_nested_functions_with_filter_outside() -> None:
    query_body = "apdex(avg(`d:transactions/duration@millisecond`), 123){status_code:418} / max(`d:transactions/duration@millisecond`){status_code:400}"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "apdex",
                (
                    FunctionCall(None, "avg", (Column("_snuba_value", "d0", "value"),)),
                    Literal(None, 123.0),
                ),
            ),
            FunctionCall(
                None,
                "max",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "418")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "d1"), Literal(None, "400")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("d1")
        + [tag_condition1, metric_condition1, tag_condition2, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_curried_aggregate_formula() -> None:
    query_body = "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            arrayElement(
                None,
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        None,
                        "quantiles",
                        (Literal(None, 0.5),),
                    ),
                    (Column("_snuba_value", "d0", "value"),),
                ),
                Literal(None, 1),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="d1", column="d1.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("d1"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("d1"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_no_groupby_no_interval_with_totals() -> None:
    mql_context_new = deepcopy(mql_context)
    mql_context_new["rollup"]["with_totals"] = "True"
    mql_context_new["rollup"]["interval"] = None
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[],
        join_type=JoinType.CROSS,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[expected_selected],
        groupby=[],
        condition=formula_condition,
        order_by=[],
        limit=1000,
        offset=0,
        totals=False,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context_new, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_onesided_groupby_no_interval_with_totals() -> None:
    mql_context_new = deepcopy(mql_context)
    mql_context_new["rollup"]["with_totals"] = "True"
    mql_context_new["rollup"]["interval"] = None
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`)"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d0", "value"),),
            ),
            FunctionCall(
                None,
                "sum",
                (Column("_snuba_value", "d1", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="d1",
            data_source=from_distributions,
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[],
        join_type=JoinType.CROSS,
        join_modifier=None,
    )

    tag_condition = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "200")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "d1")
    formula_condition = combine_and_conditions(
        condition("d0") + condition("d1") + [tag_condition, metric_condition1, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "transaction",
                subscriptable_expression("333333", "d0"),
            ),
        ],
        groupby=[
            subscriptable_expression("333333", "d0"),
        ],
        condition=formula_condition,
        order_by=[],
        limit=1000,
        offset=0,
        totals=True,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context_new, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_extrapolation_with_nested_functions() -> None:
    query_body = "apdex(avg(`d:transactions/duration@millisecond`){status_code:418}, 123) / sum(`c:transactions/duration@millisecond`){status_code:400}"

    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "apdex",
                (
                    FunctionCall(None, "avg_weighted", (Column("_snuba_value", "d0", "value"),)),
                    Literal(None, 123.0),
                ),
            ),
            FunctionCall(
                None,
                "sum_weighted",
                (Column("_snuba_value", "c0", "value"),),
            ),
            "_snuba_aggregate_value",
        ),
    )

    join_clause = JoinClause(
        left_node=IndividualNode(
            alias="c0",
            data_source=QueryEntity(
                EntityKey.GENERIC_METRICS_COUNTERS,
                get_entity(EntityKey.GENERIC_METRICS_COUNTERS).get_data_model(),
            ),
        ),
        right_node=IndividualNode(
            alias="d0",
            data_source=from_distributions,
        ),
        keys=[
            JoinCondition(
                left=JoinConditionExpression(table_alias="c0", column="c0.time"),
                right=JoinConditionExpression(table_alias="d0", column="d0.time"),
            )
        ],
        join_type=JoinType.INNER,
        join_modifier=None,
    )

    tag_condition1 = binary_condition(
        "equals", tag_column("status_code", "d0"), Literal(None, "418")
    )
    tag_condition2 = binary_condition(
        "equals", tag_column("status_code", "c0"), Literal(None, "400")
    )
    metric_condition1 = metric_id_condition(123456, "d0")
    metric_condition2 = metric_id_condition(123456, "c0")
    formula_condition = combine_and_conditions(
        condition("d0")
        + condition("c0")
        + [tag_condition1, metric_condition1, tag_condition2, metric_condition2]
    )

    expected = CompositeQuery(
        from_clause=join_clause,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression("c0"),
            ),
            SelectedExpression(
                "time",
                time_expression("d0"),
            ),
        ],
        groupby=[time_expression("c0"), time_expression("d0")],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )

    mql_context_with_extrapolation = deepcopy(mql_context)
    mql_context_with_extrapolation["extrapolate"] = True
    mql_context_with_extrapolation["indexer_mappings"][
        "c:transactions/duration@millisecond"
    ] = 123456

    query = parse_mql_query(str(query_body), mql_context_with_extrapolation, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason
