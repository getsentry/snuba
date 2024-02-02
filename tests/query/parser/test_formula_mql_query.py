from __future__ import annotations

import re
from datetime import datetime

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import divide, multiply, plus
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query

# Commonly used expressions
from_distributions = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
)

time_expression = FunctionCall(
    "_snuba_time",
    "toStartOfInterval",
    (
        Column("_snuba_timestamp", None, "timestamp"),
        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
        Literal(None, "Universal"),
    ),
)

formula_condition = FunctionCall(
    None,
    "and",
    (
        FunctionCall(
            None,
            "equals",
            (
                Column(
                    "_snuba_granularity",
                    None,
                    "granularity",
                ),
                Literal(None, 60),
            ),
        ),
        FunctionCall(
            None,
            "and",
            (
                FunctionCall(
                    None,
                    "in",
                    (
                        Column(
                            "_snuba_project_id",
                            None,
                            "project_id",
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (Literal(None, 11),),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    "and",
                    (
                        FunctionCall(
                            None,
                            "in",
                            (
                                Column(
                                    "_snuba_org_id",
                                    None,
                                    "org_id",
                                ),
                                FunctionCall(
                                    None,
                                    "tuple",
                                    (Literal(None, 1),),
                                ),
                            ),
                        ),
                        FunctionCall(
                            None,
                            "and",
                            (
                                FunctionCall(
                                    None,
                                    "equals",
                                    (
                                        Column(
                                            "_snuba_use_case_id",
                                            None,
                                            "use_case_id",
                                        ),
                                        Literal(None, "transactions"),
                                    ),
                                ),
                                FunctionCall(
                                    None,
                                    "and",
                                    (
                                        FunctionCall(
                                            None,
                                            "greaterOrEquals",
                                            (
                                                Column(
                                                    "_snuba_timestamp",
                                                    None,
                                                    "timestamp",
                                                ),
                                                Literal(
                                                    None,
                                                    datetime(2023, 11, 23, 18, 30),
                                                ),
                                            ),
                                        ),
                                        FunctionCall(
                                            None,
                                            "less",
                                            (
                                                Column(
                                                    "_snuba_timestamp",
                                                    None,
                                                    "timestamp",
                                                ),
                                                Literal(
                                                    None,
                                                    datetime(
                                                        2023,
                                                        11,
                                                        23,
                                                        22,
                                                        30,
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    ),
)
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
    agg: str, metric_id: int | None = None, condition: FunctionCall | None = None
) -> FunctionCall:
    if metric_id:
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
    return (
        FunctionCall(
            "_snuba_aggregate_value",
            agg,
            (Column("_snuba_value", None, "value"),),
        ),
    )


def tag_column(tag: str) -> SubscriptableReference:
    tag_val = mql_context.get("indexer_mappings").get(tag)  # type: ignore
    return SubscriptableReference(
        alias=f"_snuba_tags_raw[{tag_val}]",
        column=Column(
            alias="_snuba_tags_raw",
            table_name=None,
            column_name="tags_raw",
        ),
        key=Literal(alias=None, value=f"{tag_val}"),
    )


def test_simple_formula() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            timeseries(
                "sumIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            timeseries("sumIf", 123456),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            timeseries(
                "sumIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            timeseries("sumIf", 123456),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression("transaction", tag_column("transaction")),
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[tag_column("transaction"), time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_curried_aggregate() -> None:
    query_body = "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            CurriedFunctionCall(
                alias=None,
                internal_function=FunctionCall(
                    None, "quantilesIf", (Literal(None, 0.5),)
                ),
                parameters=(
                    Column("_snuba_value", None, "value"),
                    FunctionCall(
                        None,
                        "and",
                        (
                            binary_condition(
                                "equals",
                                tag_column("status_code"),
                                Literal(None, "200"),
                            ),
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column(
                                        "_snuba_metric_id",
                                        None,
                                        "metric_id",
                                    ),
                                    Literal(None, 123456),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            timeseries("sumIf", 123456),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression("transaction", tag_column("transaction")),
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[tag_column("transaction"), time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_bracketing_rules() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            timeseries("sumIf", 123456),
            multiply(
                plus(
                    timeseries("maxIf", 123456),
                    timeseries("avgIf", 123456),
                ),
                timeseries("minIf", 123456),
            ),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
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


def test_formula_filters() -> None:
    query_body = "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200}"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            timeseries(
                "sumIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            timeseries(
                "maxIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_groupby() -> None:
    query_body = "(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`)){status_code:200} by transaction"
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            timeseries(
                "sumIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            timeseries(
                "maxIf",
                123456,
                binary_condition(
                    "equals", tag_column("status_code"), Literal(None, "200")
                ),
            ),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                name="transaction",
                expression=tag_column("transaction"),
            ),
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[tag_column("transaction"), time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_formula_scalar_value() -> None:
    query_body = "(sum(`d:transactions/duration@millisecond`) / sum(`d:transactions/duration@millisecond`)) + 100"
    expected_selected = SelectedExpression(
        "aggregate_value",
        plus(
            divide(
                timeseries("sumIf", 123456),
                timeseries("sumIf", 123456),
            ),
            Literal(None, 100),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


@pytest.mark.xfail(reason="Not implemented yet")  # type: ignore
def test_arbitrary_functions() -> None:
    query_body = "apdex(sum(`d:transactions/duration@millisecond`), 123) / max(`d:transactions/duration@millisecond`)"

    # Note: This expected selected might not be correct, depending on exactly how we build this
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "apdex",
                (
                    Literal(None, "d:transactions/duration@millisecond"),
                    Literal(None, 123),
                ),
            ),
            timeseries("maxIf", 123456),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


@pytest.mark.xfail(reason="Not implemented yet")  # type: ignore
def test_arbitrary_functions_with_formula() -> None:
    query_body = "apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 123)"

    # Note: This expected selected might not be correct, depending on exactly how we build this
    expected_selected = SelectedExpression(
        "aggregate_value",
        divide(
            FunctionCall(
                None,
                "apdex",
                (
                    Literal(None, "d:transactions/duration@millisecond"),
                    Literal(None, 123),
                ),
            ),
            timeseries("maxIf", 123456),
            "_snuba_aggregate_value",
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


@pytest.mark.xfail(reason="Not implemented yet = needs snuba-sdk>2.0.20")  # type: ignore
def test_arbitrary_functions_with_formula_and_filters() -> None:
    query_body = 'apdex(sum(`d:transactions/duration@millisecond`) / max(`d:transactions/duration@millisecond`), 500){dist:["dist1", "dist2"]}'

    # Note: This expected selected might not be correct, depending on exactly how we build this
    expected_selected = SelectedExpression(
        "aggregate_value",
        FunctionCall(
            "_snuba_aggregate_value",
            "apdex",
            (
                divide(
                    timeseries("sumIf", 123456),
                    timeseries("maxIf", 123456),
                ),
                Literal(None, 500),
            ),
        ),
    )
    expected = Query(
        from_distributions,
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                time_expression,
            ),
        ],
        groupby=[time_expression],
        condition=binary_condition(
            "and",
            formula_condition,
            binary_condition(
                "in",
                tag_column("dist"),
                FunctionCall(
                    None, "array", (Literal(None, "dist1"), Literal(None, "dist2"))
                ),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression,
            )
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_multi_type_formula() -> None:
    query_body = "(sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/measurements.fp@millisecond`)) + count(`s:transactions/user@none`)"
    mql_context = {
        "entity": {
            "d:transactions/duration@millisecond": "generic_metrics_distributions",
            "d:transactions/measurements.fp@millisecond": "generic_metrics_distributions",
            "s:transactions/user@none": "generic_metrics_sets",
        },
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
            "d:transactions/measurements.fp@millisecond": 234567,
            "s:transactions/user@none": 789012,
            "status_code": 111111,
        },
        "limit": None,
        "offset": None,
    }

    expected_selected = SelectedExpression(
        "aggregate_value",
        plus(
            divide(
                FunctionCall(
                    "_snuba_aggregate_value",
                    "sumIf",
                    (
                        Column(
                            "_snuba_generic_metrics_distributions_value",
                            "generic_metrics_distributions",
                            "value",
                        ),
                        FunctionCall(
                            None,
                            "and",
                            (
                                FunctionCall(
                                    None,
                                    "equals",
                                    (
                                        SubscriptableReference(
                                            alias="_snuba_tags_raw[111111]",
                                            column=Column(
                                                alias="_snuba_tags_raw",
                                                table_name=None,
                                                column_name="tags_raw",
                                            ),
                                            key=Literal(alias=None, value="111111"),
                                        ),
                                        Literal(None, 200),
                                    ),
                                ),
                                FunctionCall(
                                    None,
                                    "equals",
                                    (
                                        Column(
                                            "_snuba_generic_metrics_distributions_metric_id",
                                            "generic_metrics_distributions",
                                            "metric_id",
                                        ),
                                        Literal(None, 123456),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                FunctionCall(
                    "_snuba_aggregate_value",
                    "sumIf",
                    (
                        Column(
                            "_snuba_generic_metrics_distributions_value",
                            "generic_metrics_distributions",
                            "value",
                        ),
                        FunctionCall(
                            None,
                            "equals",
                            (
                                Column(
                                    "_snuba_generic_metrics_distributions_metric_id",
                                    "generic_metrics_distributions",
                                    "metric_id",
                                ),
                                Literal(None, 234567),
                            ),
                        ),
                    ),
                ),
            ),
            FunctionCall(
                "_snuba_aggregate_value",
                "count",
                (
                    Column(
                        "_snuba_generic_metrics_sets_value",
                        "generic_metrics_sets",
                        "value",
                    ),
                ),
            ),
            "_snuba_aggregate_value",
        ),
    )
    expected = CompositeQuery(
        from_clause=JoinClause(
            left_node=IndividualNode(
                "generic_metrics_distributions",
                QueryEntity(
                    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                    get_entity(
                        EntityKey.GENERIC_METRICS_DISTRIBUTIONS
                    ).get_data_model(),
                ),
            ),
            right_node=IndividualNode(
                "generic_metrics_sets",
                QueryEntity(
                    EntityKey.GENERIC_METRICS_SETS,
                    get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
                ),
            ),
            keys=[
                JoinCondition(
                    JoinConditionExpression(
                        "generic_metrics_distributions", "timestamp"
                    ),
                    JoinConditionExpression("generic_metrics_sets", "timestamp"),
                )
            ],
            join_type=JoinType.INNER,
        ),
        selected_columns=[
            expected_selected,
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        Column(
                            "_snuba_timestamp",
                            "generic_metrics_distributions",
                            "timestamp",
                        ),
                        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
                        Literal(None, "Universal"),
                    ),
                ),
            ),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        Column(
                            "_snuba_timestamp",
                            "generic_metrics_sets",
                            "timestamp",
                        ),
                        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
                        Literal(None, "Universal"),
                    ),
                ),
            ),
        ],
        condition=formula_condition,
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        Column(
                            "_snuba_timestamp",
                            "generic_metrics_distributions",
                            "timestamp",
                        ),
                        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
                        Literal(None, "Universal"),
                    ),
                ),
            ),
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        Column("_snuba_timestamp", "generic_metrics_sets", "timestamp"),
                        FunctionCall(None, "toIntervalSecond", (Literal(None, 60),)),
                        Literal(None, "Universal"),
                    ),
                ),
            ),
        ],
        limit=1000,
        offset=0,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason
