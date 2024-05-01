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
from snuba.query.dsl import (
    and_cond,
    arrayElement,
    column,
    divide,
    equals,
    greaterOrEquals,
    in_fn,
    less,
    literal,
    literals_tuple,
    multiply,
    or_cond,
    plus,
    snuba_tags_raw,
)
from snuba.query.dsl_mapper import query_repr
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query

# from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query

# import pytest


# Commonly used expressions
from_distributions = from_clause = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
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


def test_simple_formula() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    expected = Query(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                divide(
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    "_snuba_aggregate_value",
                ),
            ),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            greaterOrEquals(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 18, 30)),
            ),
            less(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 22, 30)),
            ),
            in_fn(
                column("project_id", None, "_snuba_project_id"),
                literals_tuple(None, [literal(11)]),
            ),
            in_fn(
                column("org_id", None, "_snuba_org_id"),
                literals_tuple(None, [literal(1)]),
            ),
            equals(
                column("use_case_id", None, "_snuba_use_case_id"),
                literal("transactions"),
            ),
            equals(column("granularity", None, "_snuba_granularity"), literal(60)),
            equals(snuba_tags_raw(int(222222)), literal("200")),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
        ),
        groupby=[
            FunctionCall(
                "_snuba_time",
                "toStartOfInterval",
                (
                    column("timestamp", None, "_snuba_timestamp"),
                    FunctionCall(None, "toIntervalSecond", (literal(60),)),
                    literal("Universal"),
                ),
            )
        ],
        having=None,
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            )
        ],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_simple_formula_with_leading_literals() -> None:
    query_body = "1 + sum(`d:transactions/duration@millisecond`){status_code:200} / sum(`d:transactions/duration@millisecond`)"
    expected = Query(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                plus(
                    literal(1.0),
                    divide(
                        FunctionCall(
                            None, "sum", (column("value", None, "_snuba_value"),)
                        ),
                        FunctionCall(
                            None, "sum", (column("value", None, "_snuba_value"),)
                        ),
                    ),
                    "_snuba_aggregate_value",
                ),
            ),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            greaterOrEquals(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 18, 30)),
            ),
            less(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 22, 30)),
            ),
            in_fn(
                column("project_id", None, "_snuba_project_id"),
                literals_tuple(None, [literal(11)]),
            ),
            in_fn(
                column("org_id", None, "_snuba_org_id"),
                literals_tuple(None, [literal(1)]),
            ),
            equals(
                column("use_case_id", None, "_snuba_use_case_id"),
                literal("transactions"),
            ),
            equals(column("granularity", None, "_snuba_granularity"), literal(60)),
            equals(snuba_tags_raw(int(222222)), literal("200")),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
        ),
        groupby=[
            FunctionCall(
                "_snuba_time",
                "toStartOfInterval",
                (
                    column("timestamp", None, "_snuba_timestamp"),
                    FunctionCall(None, "toIntervalSecond", (literal(60),)),
                    literal("Universal"),
                ),
            )
        ],
        having=None,
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            )
        ],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )

    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_groupby() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"
    expected = Query(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                divide(
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    "_snuba_aggregate_value",
                ),
            ),
            SelectedExpression("transaction", snuba_tags_raw(int(333333))),
            SelectedExpression("transaction", snuba_tags_raw(int(333333))),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            greaterOrEquals(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 18, 30)),
            ),
            less(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 22, 30)),
            ),
            in_fn(
                column("project_id", None, "_snuba_project_id"),
                literals_tuple(None, [literal(11)]),
            ),
            in_fn(
                column("org_id", None, "_snuba_org_id"),
                literals_tuple(None, [literal(1)]),
            ),
            equals(
                column("use_case_id", None, "_snuba_use_case_id"),
                literal("transactions"),
            ),
            equals(column("granularity", None, "_snuba_granularity"), literal(60)),
            equals(snuba_tags_raw(int(222222)), literal("200")),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
        ),
        groupby=[
            snuba_tags_raw(int(333333)),
            snuba_tags_raw(int(333333)),
            FunctionCall(
                "_snuba_time",
                "toStartOfInterval",
                (
                    column("timestamp", None, "_snuba_timestamp"),
                    FunctionCall(None, "toIntervalSecond", (literal(60),)),
                    literal("Universal"),
                ),
            ),
        ],
        having=None,
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            )
        ],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )
    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_curried_aggregate() -> None:
    query_body = "quantiles(0.5)(`d:transactions/duration@millisecond`){status_code:200} by transaction / sum(`d:transactions/duration@millisecond`) by transaction"
    expected = Query(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                divide(
                    arrayElement(
                        None,
                        CurriedFunctionCall(
                            None,
                            FunctionCall(None, "quantiles", (literal(0.5),)),
                            (column("value", None, "_snuba_value"),),
                        ),
                        literal(1),
                    ),
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    "_snuba_aggregate_value",
                ),
            ),
            SelectedExpression("transaction", snuba_tags_raw(int(333333))),
            SelectedExpression("transaction", snuba_tags_raw(int(333333))),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            greaterOrEquals(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 18, 30)),
            ),
            less(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 22, 30)),
            ),
            in_fn(
                column("project_id", None, "_snuba_project_id"),
                literals_tuple(None, [literal(11)]),
            ),
            in_fn(
                column("org_id", None, "_snuba_org_id"),
                literals_tuple(None, [literal(1)]),
            ),
            equals(
                column("use_case_id", None, "_snuba_use_case_id"),
                literal("transactions"),
            ),
            equals(column("granularity", None, "_snuba_granularity"), literal(60)),
            equals(snuba_tags_raw(int(222222)), literal("200")),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
        ),
        groupby=[
            snuba_tags_raw(int(333333)),
            snuba_tags_raw(int(333333)),
            FunctionCall(
                "_snuba_time",
                "toStartOfInterval",
                (
                    column("timestamp", None, "_snuba_timestamp"),
                    FunctionCall(None, "toIntervalSecond", (literal(60),)),
                    literal("Universal"),
                ),
            ),
        ],
        having=None,
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            )
        ],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )
    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


def test_bracketing_rules() -> None:
    query_body = "sum(`d:transactions/duration@millisecond`) / ((max(`d:transactions/duration@millisecond`) + avg(`d:transactions/duration@millisecond`)) * min(`d:transactions/duration@millisecond`))"
    expected = Query(
        from_clause=from_clause,
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                divide(
                    FunctionCall(None, "sum", (column("value", None, "_snuba_value"),)),
                    multiply(
                        plus(
                            FunctionCall(
                                None, "max", (column("value", None, "_snuba_value"),)
                            ),
                            FunctionCall(
                                None, "avg", (column("value", None, "_snuba_value"),)
                            ),
                        ),
                        FunctionCall(
                            None, "min", (column("value", None, "_snuba_value"),)
                        ),
                    ),
                    "_snuba_aggregate_value",
                ),
            ),
            SelectedExpression(
                "time",
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            ),
        ],
        array_join=None,
        condition=and_cond(
            greaterOrEquals(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 18, 30)),
            ),
            less(
                column("timestamp", None, "_snuba_timestamp"),
                literal(datetime(2023, 11, 23, 22, 30)),
            ),
            in_fn(
                column("project_id", None, "_snuba_project_id"),
                literals_tuple(None, [literal(11)]),
            ),
            in_fn(
                column("org_id", None, "_snuba_org_id"),
                literals_tuple(None, [literal(1)]),
            ),
            equals(
                column("use_case_id", None, "_snuba_use_case_id"),
                literal("transactions"),
            ),
            equals(column("granularity", None, "_snuba_granularity"), literal(60)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
            equals(column("metric_id", None, "_snuba_metric_id"), literal(123456)),
        ),
        groupby=[
            FunctionCall(
                "_snuba_time",
                "toStartOfInterval",
                (
                    column("timestamp", None, "_snuba_timestamp"),
                    FunctionCall(None, "toIntervalSecond", (literal(60),)),
                    literal("Universal"),
                ),
            )
        ],
        having=None,
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_time",
                    "toStartOfInterval",
                    (
                        column("timestamp", None, "_snuba_timestamp"),
                        FunctionCall(None, "toIntervalSecond", (literal(60),)),
                        literal("Universal"),
                    ),
                ),
            )
        ],
        limitby=None,
        limit=1000,
        offset=0,
        totals=False,
        granularity=None,
    )
    generic_metrics = get_dataset(
        "generic_metrics",
    )
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
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


@pytest.mark.xfail()
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
    filter_in_select_condition = or_cond(
        and_cond(
            equals(
                tag_column("status_code"),
                Literal(None, "200"),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
        ),
        and_cond(
            equals(
                tag_column("status_code"),
                Literal(None, "200"),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
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
            filter_in_select_condition,
            formula_condition,
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


@pytest.mark.xfail()
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
    filter_in_select_condition = or_cond(
        and_cond(
            equals(
                tag_column("status_code"),
                Literal(None, "200"),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
        ),
        and_cond(
            equals(
                tag_column("status_code"),
                Literal(None, "200"),
            ),
            equals(
                Column("_snuba_metric_id", None, "metric_id"),
                Literal(None, 123456),
            ),
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
        condition=binary_condition(
            "and",
            filter_in_select_condition,
            formula_condition,
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason


@pytest.mark.xfail()
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
    filter_in_select_condition = or_cond(
        equals(
            Column("_snuba_metric_id", None, "metric_id"),
            Literal(None, 123456),
        ),
        equals(
            Column("_snuba_metric_id", None, "metric_id"),
            Literal(None, 123456),
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
            filter_in_select_condition,
            formula_condition,
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
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
    query = parse_mql_query(str(query_body), mql_context, generic_metrics)
    eq, reason = query.equals(expected)
    assert eq, reason
