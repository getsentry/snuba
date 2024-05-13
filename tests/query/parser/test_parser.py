"""
These are the end-to-end tests of the parsing pipeline as a whole. (parse_mql_query, parse_snql_query)
The number of tests in this file should be kept to a minimum, just
a small number of representative cases for the entire pipeline.

If you have a new component to the pipeline to test, you should test it in isolation
by creating unit tests in ./unit_tests
"""

from datetime import datetime
from typing import Optional

import pytest

from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import BooleanFunctions, binary_condition
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import NestedColumn, and_cond, divide, equals, or_cond
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.mql.parser import parse_mql_query
from snuba.query.snql.parser import parse_snql_query

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")
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


def test_mql() -> None:
    mql = 'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}'
    context = {
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
            "d:transactions/duration@millisecond": 123456,
            "dist": 888,
        },
    }
    expected = Query(
        QueryEntity(
            EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
            get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
        ),
        selected_columns=[
            SelectedExpression(
                "aggregate_value",
                FunctionCall(
                    "_snuba_aggregate_value",
                    "sum",
                    (Column("_snuba_value", None, "value"),),
                ),
            ),
        ],
        groupby=[],
        condition=FunctionCall(
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
                                                            datetime(2021, 1, 1, 0, 0),
                                                        ),
                                                    ),
                                                ),
                                                FunctionCall(
                                                    None,
                                                    "and",
                                                    (
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
                                                                        2021,
                                                                        1,
                                                                        2,
                                                                        0,
                                                                        0,
                                                                    ),
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
                                                                            "_snuba_metric_id",
                                                                            None,
                                                                            "metric_id",
                                                                        ),
                                                                        Literal(
                                                                            None,
                                                                            123456,
                                                                        ),
                                                                    ),
                                                                ),
                                                                FunctionCall(
                                                                    None,
                                                                    "in",
                                                                    (
                                                                        SubscriptableReference(
                                                                            "_snuba_tags_raw[888]",
                                                                            column=Column(
                                                                                "_snuba_tags_raw",
                                                                                None,
                                                                                "tags_raw",
                                                                            ),
                                                                            key=Literal(
                                                                                None,
                                                                                "888",
                                                                            ),
                                                                        ),
                                                                        FunctionCall(
                                                                            None,
                                                                            "tuple",
                                                                            (
                                                                                Literal(
                                                                                    None,
                                                                                    "dist1",
                                                                                ),
                                                                                Literal(
                                                                                    None,
                                                                                    "dist2",
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
                OrderByDirection.ASC,
                FunctionCall(
                    alias="_snuba_aggregate_value",
                    function_name="sum",
                    parameters=(
                        (
                            Column(
                                alias="_snuba_value",
                                table_name=None,
                                column_name="value",
                            ),
                        )
                    ),
                ),
            ),
        ],
        limit=1000,
    )
    actual = parse_mql_query(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
    assert eq, reason


def test_formula_mql() -> None:
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


def test_snql() -> None:
    def with_required(condition: Optional[Expression] = None) -> Expression:
        required = binary_condition(
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
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                FunctionCall(
                    None,
                    "equals",
                    (
                        Column("_snuba_project_id", None, "project_id"),
                        Literal(None, 1),
                    ),
                ),
            ),
        )

        if condition:
            return binary_condition(BooleanFunctions.AND, condition, required)

        return required

    DEFAULT_TEST_QUERY_CONDITIONS = [
        "timestamp >= toDateTime('2021-01-01T00:00:00')",
        "timestamp < toDateTime('2021-01-02T00:00:00')",
        "project_id = 1",
    ]

    def snql_conditions_with_default(*conditions: str) -> str:
        return " AND ".join(list(conditions) + DEFAULT_TEST_QUERY_CONDITIONS)

    snql = """
MATCH (events)
SELECT group_id, goo(partition) AS issue_id,
        foo(zoo(offset)) AS offset
WHERE {conditions}
ORDER BY group_id ASC
""".format(
        conditions=snql_conditions_with_default("foo(issue_id) AS group_id = 1")
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression(
                "group_id",
                FunctionCall(
                    "_snuba_group_id",
                    "foo",
                    (
                        FunctionCall(
                            "_snuba_issue_id",
                            "goo",
                            (Column("_snuba_partition", None, "partition"),),
                        ),
                    ),
                ),
            ),
            SelectedExpression(
                "issue_id",
                FunctionCall(
                    "_snuba_issue_id",
                    "goo",
                    (Column("_snuba_partition", None, "partition"),),
                ),
            ),
            SelectedExpression(
                "offset",
                FunctionCall(
                    "_snuba_offset",
                    "foo",
                    (FunctionCall(None, "zoo", (Column(None, None, "offset"),)),),
                ),
            ),
        ],
        condition=with_required(
            binary_condition(
                "equals",
                FunctionCall(
                    "_snuba_group_id",
                    "foo",
                    (
                        FunctionCall(
                            "_snuba_issue_id",
                            "goo",
                            (Column("_snuba_partition", None, "partition"),),
                        ),
                    ),
                ),
                Literal(None, 1),
            )
        ),
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    "_snuba_group_id",
                    "foo",
                    (
                        FunctionCall(
                            "_snuba_issue_id",
                            "goo",
                            (Column("_snuba_partition", None, "partition"),),
                        ),
                    ),
                ),
            ),
        ],
        limit=1000,
    )
    actual = parse_snql_query(str(snql), get_dataset("events"))
    eq, reason = actual.equals(expected)
    assert eq, reason


@pytest.mark.skip()
def test_snql_composite() -> None:
    """
    We currently have no composite queries in our parse tests... aparently.
    """
    pass


@pytest.mark.skip()
def test_custom_processing() -> None:
    """
    We also currently have no tests that use custom processing to my knowledge.
    """
    pass
