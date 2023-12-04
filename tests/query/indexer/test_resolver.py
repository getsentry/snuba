from __future__ import annotations

from typing import Any, Mapping, Union

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.indexer.resolver import (
    resolve_gropupby_processor,
    resolve_metric_id_processor,
    resolve_tag_filters_processor,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.mql.mql_context import MQLContext

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
        {"mri": "d:transactions/duration@millisecond"},
        MQLContext(indexer_mappings={"d:transactions/duration@millisecond": "123456"}),
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
        {"public_name": "transaction.user"},
        MQLContext(
            indexer_mappings={
                "transaction.user": "s:transactions/user@none",
                "s:transactions/user@none": "567890",
            }
        ),
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
                    Literal(None, "567890"),
                ),
            ),
            limit=1000,
            offset=0,
        ),
        id="resolve public name to metric id condition",
    ),
]


@pytest.mark.parametrize(
    "query, parsed, mql_context, expected_query", metric_id_test_cases
)
def test_resolve_metric_id_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
    expected_query: Union[CompositeQuery[QueryEntity], LogicalQuery],
) -> None:
    resolve_metric_id_processor(query, parsed, mql_context)
    assert query == expected_query


groupby_test_cases = [
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
        {
            "groupby": Column(
                None,
                None,
                "transactions",
            ),
        },
        MQLContext(indexer_mappings={"transactions": "111111"}),
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
]


@pytest.mark.parametrize(
    "query, parsed, mql_context, expected_query", groupby_test_cases
)
def test_resolve_gropupby_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
    expected_query: Union[CompositeQuery[QueryEntity], LogicalQuery],
) -> None:
    resolve_gropupby_processor(query, parsed, mql_context)
    assert query == expected_query


tag_filter_test_cases = [
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
        {
            "filters": [
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
            ]
        },
        MQLContext(indexer_mappings={"foo": "999999"}),
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
        {
            "filters": [
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
            ]
        },
        MQLContext(indexer_mappings={"foo": "999999", "dist": "888888"}),
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


@pytest.mark.parametrize(
    "query, parsed, mql_context, expected_query", tag_filter_test_cases
)
def test_resolve_tag_filters_processor(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    parsed: Mapping[str, Any],
    mql_context: MQLContext,
    expected_query: Union[CompositeQuery[QueryEntity], LogicalQuery],
) -> None:
    resolve_tag_filters_processor(query, parsed, mql_context)
    assert query == expected_query
