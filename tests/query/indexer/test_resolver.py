from __future__ import annotations

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.indexer.resolver import (
    resolve_tag_key_mappings,
    resolve_tag_value_mappings,
)
from snuba.query.logical import Query as LogicalQuery

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
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"d:transactions/duration@millisecond": "123456"},
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
                    Literal(None, "123456"),
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
]


@pytest.mark.skip(reason="resolver is not being used yet")
@pytest.mark.parametrize("query, mappings, expected_query", metric_id_test_cases)
def test_resolve_tag_value_mappings_processor(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
    expected_query: CompositeQuery[QueryEntity] | LogicalQuery,
) -> None:
    resolve_tag_value_mappings(query, mappings)
    assert query == expected_query


tag_test_cases = [
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
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    Column(
                        None,
                        None,
                        "transactions",
                    ),
                ),
            ],
            groupby=[
                Column(
                    None,
                    None,
                    "transactions",
                )
            ],
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"transactions": "111111"},
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
                SelectedExpression(
                    "quantiles(0.5, 0.75)(transaction.user)",
                    Column(
                        "transactions",
                        None,
                        "tags_raw[111111]",
                    ),
                ),
            ],
            granularity=60,
            condition=None,
            groupby=[
                Column(
                    "transactions",
                    None,
                    "tags_raw[111111]",
                ),
            ],
            limit=1000,
            offset=0,
        ),
        id="resolve group by column and selected column",
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
            condition=FunctionCall(
                None,
                "equals",
                (
                    Column(
                        "foo",
                        None,
                        "foo",
                    ),
                    Literal(None, "bar"),
                ),
            ),
            granularity=60,
            limit=1000,
            offset=0,
        ),
        {"foo": "999999"},
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
                    Column(
                        "foo",
                        None,
                        "tags_raw[999999]",
                    ),
                    Literal(None, "bar"),
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
                FunctionCall(
                    None,
                    "notIn",
                    (
                        Column(
                            "dist",
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
                            "foo",
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
        {"foo": "999999", "dist": "888888"},
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
                FunctionCall(
                    None,
                    "notIn",
                    (
                        Column(
                            "dist",
                            None,
                            "tags_raw[888888]",
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
                            "foo",
                            None,
                            "tags_raw[999999]",
                        ),
                        Literal(None, "bar"),
                    ),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve multiple tag filters",
    ),
]


@pytest.mark.skip(reason="resolver is not being used yet")
@pytest.mark.parametrize("query, mappings, expected_query", tag_test_cases)
def test_resolve_tag_key_mappings_processor(
    query: CompositeQuery[QueryEntity] | LogicalQuery,
    mappings: dict[str, str | int],
    expected_query: CompositeQuery[QueryEntity] | LogicalQuery,
) -> None:
    resolve_tag_key_mappings(query, mappings)
    assert query == expected_query
