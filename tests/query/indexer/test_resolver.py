from __future__ import annotations

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import divide
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.indexer.resolver import resolve_mappings
from snuba.query.logical import Query as LogicalQuery

"""
NOTES ABOUT THESE TESTS

The resolver does not change columns into subscriptables, nor does it affect
aliases of any expressions. That means that some of the queries in these tests
are not exactly what would actually be passed in from the parser. However
they are specific enough to ensure the resolver is working correctly.
"""


metric_id_test_cases = [
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "metric_id"),
                    Literal(None, "d:transactions/duration@millisecond"),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"d:transactions/duration@millisecond": 123456},
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "metric_id"),
                    Literal(None, 123456),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve mri to metric id condition",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    CurriedFunctionCall(
                        "_snuba_aggregate_value",
                        FunctionCall(
                            None, "quantiles", (Literal(None, 0.5), Literal(None, 0.75))
                        ),
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "metric_id"),
                    Literal(None, "transaction.user"),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {
            "transaction.user": "s:transactions/user@none",
            "s:transactions/user@none": 567890,
        },
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    CurriedFunctionCall(
                        "_snuba_aggregate_value",
                        FunctionCall(
                            None, "quantiles", (Literal(None, 0.5), Literal(None, 0.75))
                        ),
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "metric_id"),
                    Literal(None, 567890),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve public name to metric id condition",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(transaction.user) / sum(transaction.duration)",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "metric_id"),
                                    Literal(None, "transaction.user"),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "metric_id"),
                                    Literal(None, "transaction.duration"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "in",
                (
                    Column(None, None, "metric_id"),
                    FunctionCall(
                        None,
                        "array",
                        (
                            Literal(None, "transaction.user"),
                            Literal(None, "transaction.duration"),
                        ),
                    ),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {
            "transaction.user": "s:transactions/user@none",
            "s:transactions/user@none": 123456,
            "transaction.duration": "s:transactions/duration@none",
            "s:transactions/duration@none": 567890,
        },
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_SETS,
                get_entity(EntityKey.GENERIC_METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(transaction.user) / sum(transaction.duration)",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "metric_id"),
                                    Literal(None, 123456),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "metric_id"),
                                    Literal(None, 567890),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "in",
                (
                    Column(None, None, "metric_id"),
                    FunctionCall(
                        None,
                        "array",
                        (
                            Literal(None, 123456),
                            Literal(None, 567890),
                        ),
                    ),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        id="resolve public name to metric id in formula",
    ),
]


@pytest.mark.parametrize("query, mappings, expected_query", metric_id_test_cases)
def test_resolve_tag_value_mappings_processor(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
    expected_query: CompositeQuery[QueryEntity] | LogicalQuery,
) -> None:
    resolve_mappings(query, mappings, get_dataset("generic_metrics"))
    assert query == expected_query


tag_test_cases = [
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_SETS,
                get_entity(EntityKey.METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    CurriedFunctionCall(
                        "_snuba_aggregate_value",
                        FunctionCall(
                            None, "quantiles", (Literal(None, 0.5), Literal(None, 0.75))
                        ),
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
                SelectedExpression("transaction", Column(None, None, "transaction")),
            ],
            groupby=[Column(None, None, "transaction")],
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"transaction": 111111},
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_SETS,
                get_entity(EntityKey.METRICS_SETS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    CurriedFunctionCall(
                        "_snuba_aggregate_value",
                        FunctionCall(
                            None, "quantiles", (Literal(None, 0.5), Literal(None, 0.75))
                        ),
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    Column(None, None, "tags[111111]"),
                ),
            ],
            granularity=60,
            condition=None,
            groupby=[
                Column(None, None, "tags[111111]"),
            ],
            limit=1000,
            offset=0,
        ),
        id="resolve group by column and selected column",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "foo"),
                    Literal(None, "bar"),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"foo": 999999, "bar": 111111},
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(None, None, "tags[999999]"),
                    Literal(None, 111111),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve tag filters",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=binary_condition(
                "and",
                FunctionCall(
                    None,
                    "notIn",
                    (
                        Column(
                            None,
                            None,
                            "dist",
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (
                                Literal(None, "dist1"),
                                Literal(None, "dist2"),
                            ),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column(
                            None,
                            None,
                            "foo",
                        ),
                        Literal(None, "bar"),
                    ),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {
            "foo": 999999,
            "dist": 888888,
            "dist1": 777777,
            "dist2": 666666,
            "bar": 111111,
        },
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=binary_condition(
                "and",
                FunctionCall(
                    None,
                    "notIn",
                    (
                        Column(
                            None,
                            None,
                            "tags[888888]",
                        ),
                        FunctionCall(
                            None,
                            "tuple",
                            (
                                Literal(None, 777777),
                                Literal(None, 666666),
                            ),
                        ),
                    ),
                ),
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column(
                            None,
                            None,
                            "tags[999999]",
                        ),
                        Literal(None, 111111),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve multiple tag filters",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column(None, None, "event_type"),
                    Literal(None, "transaction"),
                ),
                binary_condition(
                    "equals", Column(None, None, "transaction"), Literal(None, "t1")
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"transaction": 999999, "event_type": 888888, "t1": 777777},
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column(None, None, "tags[888888]"),
                    Literal(None, 999999),
                ),
                binary_condition(
                    "equals", Column(None, None, "tags[999999]"), Literal(None, 777777)
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve tag keys and values",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column(None, None, "event_type"),
                    Literal(None, "transaction"),
                ),
                binary_condition(
                    "equals", Column(None, None, "transaction"), Literal(None, "t1")
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"transaction": 999999, "event_type": 888888, "t1": 777777},
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "sum(d:transactions/duration@millisecond)",
                    FunctionCall(
                        "_snuba_aggregate_value",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
            ],
            granularity=60,
            condition=binary_condition(
                "and",
                binary_condition(
                    "equals",
                    Column(None, None, "tags_raw[888888]"),
                    Literal(None, "transaction"),
                ),
                binary_condition(
                    "equals",
                    Column(None, None, "tags_raw[999999]"),
                    Literal(None, "t1"),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="only resolve tag keys despite crossover",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "(sum(user){status_code:500} / avg(duration){status_code:200}){foo:bar}",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "status_code"),
                                    Literal(None, "500"),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "avgIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "status_code"),
                                    Literal(None, "200"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            condition=binary_condition(
                "equals",
                Column(None, None, "foo"),
                Literal(None, "bar"),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {
            "user": 111111,
            "duration": 222222,
            "status_code": 333333,
            "500": 444444,
            "200": 555555,
            "foo": 666666,
            "bar": 777777,
        },
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "(sum(user){status_code:500} / avg(duration){status_code:200}){foo:bar}",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "tags_raw[333333]"),
                                    Literal(None, "500"),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "avgIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "tags_raw[333333]"),
                                    Literal(None, "200"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            granularity=60,
            condition=binary_condition(
                "equals",
                Column(None, None, "tags_raw[666666]"),
                Literal(None, "bar"),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolving with a formula in generic",
    ),
    pytest.param(
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "(sum(user){status_code:500} / avg(duration){status_code:200}){foo:bar}",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "status_code"),
                                    Literal(None, "500"),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "avgIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "status_code"),
                                    Literal(None, "200"),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            condition=binary_condition(
                "equals",
                Column(None, None, "foo"),
                Literal(None, "bar"),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {
            "user": 111111,
            "duration": 222222,
            "status_code": 333333,
            "500": 444444,
            "200": 555555,
            "foo": 666666,
            "bar": 777777,
        },
        LogicalQuery(
            from_clause=QueryEntity(
                EntityKey.METRICS_DISTRIBUTIONS,
                get_entity(EntityKey.METRICS_DISTRIBUTIONS).get_data_model(),
            ),
            selected_columns=[
                SelectedExpression(
                    "(sum(user){status_code:500} / avg(duration){status_code:200}){foo:bar}",
                    divide(
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "sumIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "tags[333333]"),
                                    Literal(None, 444444),
                                ),
                            ),
                        ),
                        FunctionCall(
                            "_snuba_aggregate_value",
                            "avgIf",
                            (
                                Column("_snuba_value", None, "value"),
                                binary_condition(
                                    "equals",
                                    Column(None, None, "tags[333333]"),
                                    Literal(None, 555555),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
            granularity=60,
            condition=binary_condition(
                "equals",
                Column(None, None, "tags[666666]"),
                Literal(None, 777777),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolving with a formula in sessions",
    ),
]


@pytest.mark.parametrize("query, mappings, expected_query", tag_test_cases)
def test_resolve_tag_key_mappings_processor(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
    expected_query: CompositeQuery[QueryEntity] | LogicalQuery,
) -> None:
    from_clause = query.get_from_clause()
    assert isinstance(from_clause, QueryEntity)
    if from_clause.key.value.startswith("generic"):
        dataset = get_dataset("generic_metrics")
    else:
        dataset = get_dataset("metrics")

    resolve_mappings(query, mappings, dataset)
    assert query == expected_query
