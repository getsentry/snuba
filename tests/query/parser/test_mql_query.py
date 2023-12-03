from datetime import datetime
from typing import Any, Dict

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query

mql_test_cases = [
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": [{"column_name": "timestamp", "direction": "ASC"}],
                "granularity": "60",
                "interval": "60",
                "with_totals": "",
            },
            "scope": {
                "org_ids": ["1"],
                "project_ids": ["1"],
                "use_case_id": "transactions",
            },
            "limit": "",
            "offset": "",
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        Query(
            QueryEntity(
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
            groupby=[],
            condition=binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column("_snuba_metric_id", None, "metric_id"),
                        Literal(None, "123456"),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    FunctionCall(
                        None,
                        "in",
                        (
                            Column("_snuba_project_id", None, "project_id"),
                            FunctionCall(None, "tuple", (Literal(None, 1),)),
                        ),
                    ),
                    binary_condition(
                        BooleanFunctions.AND,
                        FunctionCall(
                            None,
                            "in",
                            (
                                Column("_snuba_org_id", None, "org_id"),
                                FunctionCall(None, "tuple", (Literal(None, 1),)),
                            ),
                        ),
                        binary_condition(
                            BooleanFunctions.AND,
                            FunctionCall(
                                None,
                                "equals",
                                (
                                    Column("_snuba_use_case_id", None, "use_case_id"),
                                    Literal(None, "transactions"),
                                ),
                            ),
                            binary_condition(
                                BooleanFunctions.AND,
                                FunctionCall(
                                    None,
                                    "greaterOrEquals",
                                    (
                                        Column("_snuba_timestamp", None, "timestamp"),
                                        Literal(None, datetime(2021, 1, 1, 0, 0)),
                                    ),
                                ),
                                binary_condition(
                                    BooleanFunctions.AND,
                                    FunctionCall(
                                        None,
                                        "less",
                                        (
                                            Column(
                                                "_snuba_timestamp", None, "timestamp"
                                            ),
                                            Literal(None, datetime(2021, 1, 2, 0, 0)),
                                        ),
                                    ),
                                    FunctionCall(
                                        None,
                                        "in",
                                        (
                                            SubscriptableReference(
                                                "_snuba_tags_raw[000888]",
                                                Column(
                                                    "_snuba_tags_raw", None, "tags_raw"
                                                ),
                                                Literal(None, "000888"),
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
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    Column(
                        alias="_snuba_timestamp",
                        table_name=None,
                        column_name="timestamp",
                    ),
                )
            ],
            limit=1000,
            granularity=60,
        ),
        id="Select metric with filter",
    ),
    pytest.param(
        'quantiles(0.5, 0.75)(transaction.user{!dist:["dist1", "dist2"]}){foo: bar} by (transaction)',
        {
            "entity": "generic_metrics_sets",
            "start": "2021-01-01T01:36:00",
            "end": "2021-01-05T04:15:00",
            "rollup": {
                "orderby": [],
                "granularity": "3600",
                "interval": "60",
                "with_totals": "",
            },
            "scope": {
                "org_ids": ["1"],
                "project_ids": ["1"],
                "use_case_id": "transactions",
            },
            "limit": "100",
            "offset": "3",
            "indexer_mappings": {
                "transaction.user": "s:transactions/user@none",
                "s:transactions/user@none": "567890",
                "dist": "000888",
                "foo": "000777",
                "transaction": "111111",
            },
        },
        Query(
            QueryEntity(
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
                    "transaction",
                    SubscriptableReference(
                        "_snuba_tags_raw[111111]",
                        Column(
                            "_snuba_tags_raw",
                            None,
                            "tags_raw",
                        ),
                        Literal(None, "111111"),
                    ),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column("_snuba_metric_id", None, "metric_id"),
                        Literal(None, "567890"),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    FunctionCall(
                        None,
                        "in",
                        (
                            Column("_snuba_project_id", None, "project_id"),
                            FunctionCall(None, "tuple", (Literal(None, 1),)),
                        ),
                    ),
                    binary_condition(
                        BooleanFunctions.AND,
                        FunctionCall(
                            None,
                            "in",
                            (
                                Column("_snuba_org_id", None, "org_id"),
                                FunctionCall(None, "tuple", (Literal(None, 1),)),
                            ),
                        ),
                        binary_condition(
                            BooleanFunctions.AND,
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
                            binary_condition(
                                BooleanFunctions.AND,
                                FunctionCall(
                                    None,
                                    "greaterOrEquals",
                                    (
                                        Column(
                                            "_snuba_timestamp",
                                            None,
                                            "timestamp",
                                        ),
                                        Literal(None, datetime(2021, 1, 1, 1, 36)),
                                    ),
                                ),
                                binary_condition(
                                    BooleanFunctions.AND,
                                    FunctionCall(
                                        None,
                                        "less",
                                        (
                                            Column(
                                                "_snuba_timestamp",
                                                None,
                                                "timestamp",
                                            ),
                                            Literal(None, datetime(2021, 1, 5, 4, 15)),
                                        ),
                                    ),
                                    binary_condition(
                                        BooleanFunctions.AND,
                                        FunctionCall(
                                            None,
                                            "notIn",
                                            (
                                                SubscriptableReference(
                                                    "_snuba_tags_raw[000888]",
                                                    Column(
                                                        "_snuba_tags_raw",
                                                        None,
                                                        "tags_raw",
                                                    ),
                                                    Literal(None, "000888"),
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
                                                SubscriptableReference(
                                                    "_snuba_tags_raw[000777]",
                                                    Column(
                                                        "_snuba_tags_raw",
                                                        None,
                                                        "tags_raw",
                                                    ),
                                                    Literal(None, "000777"),
                                                ),
                                                Literal(None, "bar"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            order_by=[],
            groupby=[
                SubscriptableReference(
                    "_snuba_tags_raw[111111]",
                    Column(
                        "_snuba_tags_raw",
                        None,
                        "tags_raw",
                    ),
                    Literal(None, "111111"),
                )
            ],
            limit=100,
            offset=3,
            granularity=3600,
        ),
        id="Select metric with filter and groupby",
    ),
]


@pytest.mark.parametrize("query_body, mql_context, expected_query", mql_test_cases)
def test_format_expressions_from_mql(
    query_body: str, mql_context: Dict[str, Any], expected_query: Query
) -> None:
    generic_metrics = get_dataset("generic_metrics")
    query, _ = parse_mql_query(str(query_body), mql_context, generic_metrics)

    eq, reason = query.equals(expected_query)
    assert eq, reason
