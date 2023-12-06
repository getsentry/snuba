from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from snuba.query.parser.exceptions import ParsingException

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


mql_test_cases = [
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]} by (transaction, status_code)',
        {
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
                "dist": 888,
                "transaction": 111111,
                "status_code": 222222,
            },
            "limit": None,
            "offset": None,
        },
        Query(
            from_distributions,
            selected_columns=[
                SelectedExpression(
                    "aggregate_value",
                    FunctionCall(
                        "_snuba_sum(d:transactions/duration@millisecond)",
                        "sum",
                        (Column("_snuba_value", None, "value"),),
                    ),
                ),
                SelectedExpression(
                    "transaction",
                    SubscriptableReference(
                        "_snuba_tags_raw[111111]",
                        Column("_snuba_tags_raw", None, "tags_raw"),
                        Literal(None, "111111"),
                    ),
                ),
                SelectedExpression(
                    "status_code",
                    SubscriptableReference(
                        "_snuba_tags_raw[222222]",
                        Column("_snuba_tags_raw", None, "tags_raw"),
                        Literal(None, "222222"),
                    ),
                ),
                SelectedExpression(
                    "time",
                    time_expression,
                ),
            ],
            groupby=[
                SubscriptableReference(
                    "_snuba_tags_raw[111111]",
                    Column("_snuba_tags_raw", None, "tags_raw"),
                    Literal(None, "111111"),
                ),
                SubscriptableReference(
                    "_snuba_tags_raw[222222]",
                    Column("_snuba_tags_raw", None, "tags_raw"),
                    Literal(None, "222222"),
                ),
                time_expression,
            ],
            condition=FunctionCall(
                alias=None,
                function_name="and",
                parameters=(
                    FunctionCall(
                        alias=None,
                        function_name="equals",
                        parameters=(
                            Column(
                                alias="_snuba_granularity",
                                table_name=None,
                                column_name="granularity",
                            ),
                            Literal(alias=None, value=60),
                        ),
                    ),
                    FunctionCall(
                        alias=None,
                        function_name="and",
                        parameters=(
                            FunctionCall(
                                alias=None,
                                function_name="in",
                                parameters=(
                                    Column(
                                        alias="_snuba_project_id",
                                        table_name=None,
                                        column_name="project_id",
                                    ),
                                    FunctionCall(
                                        alias=None,
                                        function_name="tuple",
                                        parameters=(Literal(alias=None, value=11),),
                                    ),
                                ),
                            ),
                            FunctionCall(
                                alias=None,
                                function_name="and",
                                parameters=(
                                    FunctionCall(
                                        alias=None,
                                        function_name="in",
                                        parameters=(
                                            Column(
                                                alias="_snuba_org_id",
                                                table_name=None,
                                                column_name="org_id",
                                            ),
                                            FunctionCall(
                                                alias=None,
                                                function_name="tuple",
                                                parameters=(
                                                    Literal(alias=None, value=1),
                                                ),
                                            ),
                                        ),
                                    ),
                                    FunctionCall(
                                        alias=None,
                                        function_name="and",
                                        parameters=(
                                            FunctionCall(
                                                alias=None,
                                                function_name="equals",
                                                parameters=(
                                                    Column(
                                                        alias="_snuba_use_case_id",
                                                        table_name=None,
                                                        column_name="use_case_id",
                                                    ),
                                                    Literal(
                                                        alias=None, value="transactions"
                                                    ),
                                                ),
                                            ),
                                            FunctionCall(
                                                alias=None,
                                                function_name="and",
                                                parameters=(
                                                    FunctionCall(
                                                        alias=None,
                                                        function_name="greaterOrEquals",
                                                        parameters=(
                                                            Column(
                                                                alias="_snuba_timestamp",
                                                                table_name=None,
                                                                column_name="timestamp",
                                                            ),
                                                            Literal(
                                                                alias=None,
                                                                value=datetime(
                                                                    2023, 11, 23, 18, 30
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                    FunctionCall(
                                                        alias=None,
                                                        function_name="and",
                                                        parameters=(
                                                            FunctionCall(
                                                                alias=None,
                                                                function_name="less",
                                                                parameters=(
                                                                    Column(
                                                                        alias="_snuba_timestamp",
                                                                        table_name=None,
                                                                        column_name="timestamp",
                                                                    ),
                                                                    Literal(
                                                                        alias=None,
                                                                        value=datetime(
                                                                            2023,
                                                                            11,
                                                                            23,
                                                                            22,
                                                                            30,
                                                                        ),
                                                                    ),
                                                                ),
                                                            ),
                                                            FunctionCall(
                                                                alias=None,
                                                                function_name="and",
                                                                parameters=(
                                                                    FunctionCall(
                                                                        alias=None,
                                                                        function_name="equals",
                                                                        parameters=(
                                                                            Column(
                                                                                alias="_snuba_metric_id",
                                                                                table_name=None,
                                                                                column_name="metric_id",
                                                                            ),
                                                                            Literal(
                                                                                alias=None,
                                                                                value=123456,
                                                                            ),
                                                                        ),
                                                                    ),
                                                                    FunctionCall(
                                                                        alias=None,
                                                                        function_name="in",
                                                                        parameters=(
                                                                            SubscriptableReference(
                                                                                alias="_snuba_tags_raw[888]",
                                                                                column=Column(
                                                                                    alias="_snuba_tags_raw",
                                                                                    table_name=None,
                                                                                    column_name="tags_raw",
                                                                                ),
                                                                                key=Literal(
                                                                                    alias=None,
                                                                                    value="888",
                                                                                ),
                                                                            ),
                                                                            FunctionCall(
                                                                                alias=None,
                                                                                function_name="tuple",
                                                                                parameters=(
                                                                                    Literal(
                                                                                        alias=None,
                                                                                        value="dist1",
                                                                                    ),
                                                                                    Literal(
                                                                                        alias=None,
                                                                                        value="dist2",
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
                                ),
                            ),
                        ),
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
        ),
        id="test of resolved query",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": "ASC",
                "granularity": 60,
                "interval": None,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": None,
            "offset": None,
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
                    FunctionCall(
                        alias="_snuba_aggregate_value",
                        function_name="sum",
                        parameters=(Column("_snuba_value", None, "value"),),
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
                "orderby": None,
                "granularity": 3600,
                "interval": None,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": 100,
            "offset": 3,
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
                    "aggregate_value",
                    CurriedFunctionCall(
                        "_snuba_quantiles(0.5, 0.75)(transaction.user)",
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


invalid_mql_test_cases = [
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": None,
                "granularity": 60,
                "interval": 10,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": None,
            "offset": None,
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        ParsingException("interval 10 must be greater than or equal to granularity 6"),
        id="interval less than granularity",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": "DESC",
                "granularity": 60,
                "interval": 60,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": None,
            "offset": None,
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        ParsingException("orderby is not supported when interval is specified"),
        id="interval and orderby provided",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": None,
                "granularity": 60,
                "interval": 60,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": None,
            "offset": None,
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        InvalidQueryException("MQL context: missing required field 'entity'"),
        id="missing entity",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": None,
                "granularity": 60,
                "interval": 60,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": None,
            "offset": None,
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        ParsingException("MQL context: missing required field 'start'"),
        id="missing start time",
    ),
    pytest.param(
        'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}',
        {
            "entity": "generic_metrics_distributions",
            "start": "2021-01-01T00:00:00",
            "end": "2021-01-02T00:00:00",
            "rollup": {
                "orderby": None,
                "granularity": 60,
                "interval": 60,
                "with_totals": None,
            },
            "scope": {
                "org_ids": [1],
                "project_ids": [1],
                "use_case_id": "transactions",
            },
            "limit": 1000000,
            "offset": None,
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456",
                "dist": "000888",
            },
        },
        ParsingException("queries cannot have a limit higher than 10000"),
        id="missing limit",
    ),
]


@pytest.mark.parametrize("query_body, mql_context, error", invalid_mql_test_cases)
def test_invalid_format_expressions_from_mql(
    query_body: str,
    mql_context: Dict[str, Any],
    error: Exception,
) -> None:
    generic_metrics = get_dataset("generic_metrics")
    with pytest.raises(type(error), match=re.escape(str(error))):
        query, _ = parse_mql_query(query_body, mql_context, generic_metrics)
