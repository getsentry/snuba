from __future__ import annotations

import re
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
from snuba.query.dsl import divide, literals_tuple
from snuba.query.expressions import (
    Column,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.mql.parser_supported_join import parse_mql_query_new

# Commonly used expressions
from_distributions = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)


def time_expression(table_alias: str | None = None) -> FunctionCall:
    alias_prefix = f"{table_alias}." if table_alias else ""
    return FunctionCall(
        f"_snuba_{alias_prefix}time",
        "toStartOfInterval",
        (
            Column("_snuba_timestamp", table_alias, "timestamp"),
            FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
            Literal(None, "Universal"),
        ),
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
        "status_code": 222222,
        "transaction": 333333,
    },
    "limit": None,
    "offset": None,
}


def timeseries(
    agg: str, metric_id: int, condition: FunctionCall | None = None
) -> FunctionCall:
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
                left=JoinConditionExpression(table_alias="d0", column="time"),
                right=JoinConditionExpression(table_alias="d1", column="time"),
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
        condition("d0")
        + condition("d1")
        + [tag_condition, metric_condition1, metric_condition2]
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
    query = parse_mql_query_new(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"
    expected = ""

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query_new(str(query_body), mql_context, generic_metrics)
    print(query)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_different_groupbys() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`)"
    expected = ""

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query_new(str(query_body), mql_context, generic_metrics)
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
        parse_mql_query_new(str(query_body), mql_context, generic_metrics)


def test_onesided_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`)"
    expected = ""

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query_new(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason
