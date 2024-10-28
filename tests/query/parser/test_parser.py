"""
These are the end-to-end tests of the parsing pipeline as a whole. (parse_mql_query, parse_snql_query)
The number of tests in this file should be kept to a minimum, just
a small number of representative cases for the entire pipeline.

If you have a new component to the pipeline to test, you should test it in isolation
by creating unit tests in ./unit_tests
"""

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
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, divide, in_cond, literal
from snuba.query.expressions import (
    Column,
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


def time_expression(table_name: str = None) -> FunctionCall:
    name = f"_snuba_{table_name}.time" if table_name else "_snuba_time"
    return FunctionCall(
        name,
        "toStartOfInterval",
        (
            Column("_snuba_timestamp", table_name, "timestamp"),
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
            "with_totals": "False",
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
        condition=and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1)),
                    ),
                    in_cond(
                        column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.equals(
                        column("use_case_id", None, "_snuba_use_case_id"),
                        literal("transactions"),
                    ),
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(123456)
                    ),
                    in_cond(
                        tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
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
        totals=False,
        limit=1000,
    )
    actual = parse_mql_query(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
    assert eq, reason


def test_mql_extrapolate() -> None:
    mql = 'sum(`d:transactions/duration@millisecond`){dist:["dist1", "dist2"]}'
    context = {
        "start": "2021-01-01T00:00:00",
        "end": "2021-01-02T00:00:00",
        "rollup": {
            "orderby": "ASC",
            "granularity": 60,
            "interval": None,
            "with_totals": "False",
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
        "extrapolate": True,
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
                    "sum_weighted",
                    (Column("_snuba_value", None, "value"),),
                ),
            ),
        ],
        groupby=[],
        condition=and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1)),
                    ),
                    in_cond(
                        column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.equals(
                        column("use_case_id", None, "_snuba_use_case_id"),
                        literal("transactions"),
                    ),
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(123456)
                    ),
                    in_cond(
                        tags_raw["888"], f.tuple(literal("dist1"), literal("dist2"))
                    ),
                ),
            ),
        ),
        order_by=[
            OrderBy(
                OrderByDirection.ASC,
                FunctionCall(
                    alias="_snuba_aggregate_value",
                    function_name="sum_weighted",
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
        totals=False,
        limit=1000,
    )
    actual = parse_mql_query(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
    assert eq, reason


def test_mql_wildcards() -> None:
    mql = 'sum(`d:transactions/duration@millisecond`){mytag:"before_wildcard_*"}'
    context = {
        "start": "2021-01-01T00:00:00",
        "end": "2021-01-02T00:00:00",
        "rollup": {
            "orderby": "ASC",
            "granularity": 60,
            "interval": None,
            "with_totals": "False",
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
            "mytag": 42,
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
        condition=and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1)),
                    ),
                    in_cond(
                        column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.equals(
                        column("use_case_id", None, "_snuba_use_case_id"),
                        literal("transactions"),
                    ),
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(123456)
                    ),
                    f.like(tags_raw["42"], literal("before_wildcard_%")),
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
        totals=False,
    )
    actual = parse_mql_query(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
    assert eq, reason


def test_mql_negated_wildcards() -> None:
    mql = 'sum(`d:transactions/duration@millisecond`){!mytag:"before_wildcard_*"}'
    context = {
        "start": "2021-01-01T00:00:00",
        "end": "2021-01-02T00:00:00",
        "rollup": {
            "orderby": "ASC",
            "granularity": 60,
            "interval": None,
            "with_totals": "False",
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
            "mytag": 42,
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
        condition=and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 1, 0, 0)),
                    ),
                    f.less(
                        column("timestamp", None, "_snuba_timestamp"),
                        literal(datetime(2021, 1, 2, 0, 0)),
                    ),
                ),
                and_cond(
                    in_cond(
                        column("project_id", None, "_snuba_project_id"),
                        f.tuple(literal(1)),
                    ),
                    in_cond(
                        column("org_id", None, "_snuba_org_id"), f.tuple(literal(1))
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    f.equals(
                        column("use_case_id", None, "_snuba_use_case_id"),
                        literal("transactions"),
                    ),
                    f.equals(
                        column("granularity", None, "_snuba_granularity"), literal(60)
                    ),
                ),
                and_cond(
                    f.equals(
                        column("metric_id", None, "_snuba_metric_id"), literal(123456)
                    ),
                    f.notLike(tags_raw["42"], literal("before_wildcard_%")),
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
        totals=False,
    )
    actual = parse_mql_query(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
    assert eq, reason


def test_formula_mql() -> None:
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

    expected_join_clause = JoinClause(
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

    expected = CompositeQuery(
        expected_join_clause,
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
        condition=and_cond(
            and_cond(
                and_cond(
                    f.greaterOrEquals(
                        column("timestamp", "d0", "_snuba_timestamp"),
                        literal(datetime(2023, 11, 23, 18, 30)),
                    ),
                    and_cond(
                        f.less(
                            column("timestamp", "d0", "_snuba_timestamp"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                        in_cond(
                            column("project_id", "d0", "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        in_cond(
                            column("org_id", "d0", "_snuba_org_id"), f.tuple(literal(1))
                        ),
                        f.equals(
                            column("use_case_id", "d0", "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("granularity", "d0", "_snuba_granularity"),
                            literal(60),
                        ),
                        f.greaterOrEquals(
                            column("timestamp", "d1", "_snuba_timestamp"),
                            literal(datetime(2023, 11, 23, 18, 30)),
                        ),
                    ),
                ),
            ),
            and_cond(
                and_cond(
                    and_cond(
                        f.less(
                            column("timestamp", "d1", "_snuba_timestamp"),
                            literal(datetime(2023, 11, 23, 22, 30)),
                        ),
                        in_cond(
                            column("project_id", "d1", "_snuba_project_id"),
                            f.tuple(literal(11)),
                        ),
                    ),
                    and_cond(
                        in_cond(
                            column("org_id", "d1", "_snuba_org_id"),
                            f.tuple(literal(1)),
                        ),
                        f.equals(
                            column("use_case_id", "d1", "_snuba_use_case_id"),
                            literal("transactions"),
                        ),
                    ),
                ),
                and_cond(
                    and_cond(
                        f.equals(
                            column("granularity", "d1", "_snuba_granularity"),
                            literal(60),
                        ),
                        f.equals(
                            NestedColumn("tags_raw", "d0")["222222"], literal("200")
                        ),
                    ),
                    and_cond(
                        f.equals(
                            column("metric_id", "d0", "_snuba_metric_id"),
                            literal(123456),
                        ),
                        f.equals(
                            column("metric_id", "d1", "_snuba_metric_id"),
                            literal(123456),
                        ),
                    ),
                ),
            ),
        ),
        order_by=[
            OrderBy(
                direction=OrderByDirection.ASC,
                expression=time_expression("d0"),
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
        condition=and_cond(
            and_cond(
                f.equals(
                    f.foo(
                        f.goo(
                            column("partition", None, "_snuba_partition"),
                            alias="_snuba_issue_id",
                        ),
                        alias="_snuba_group_id",
                    ),
                    literal(1),
                ),
                f.greaterOrEquals(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 1, 0, 0)),
                ),
            ),
            and_cond(
                f.less(
                    column("timestamp", None, "_snuba_timestamp"),
                    literal(datetime(2021, 1, 2, 0, 0)),
                ),
                f.equals(column("project_id", None, "_snuba_project_id"), literal(1)),
            ),
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


def test_recursion_error() -> None:
    NUM_CONDS = 500

    conds = " OR ".join(["a:1" for i in range(NUM_CONDS)])
    mql = f"sum(`d:transactions/duration@millisecond`){{{conds}}}"
    context = {
        "start": "2021-01-01T00:00:00",
        "end": "2021-01-02T00:00:00",
        "rollup": {
            "orderby": "ASC",
            "granularity": 60,
            "interval": None,
            "with_totals": "True",
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
    parse_mql_query(mql, context, get_dataset("generic_metrics"))

    def snql_conditions_with_default(*conditions: str) -> str:
        DEFAULT_TEST_QUERY_CONDITIONS = [
            "timestamp >= toDateTime('2021-01-01T00:00:00')",
            "timestamp < toDateTime('2021-01-02T00:00:00')",
            "project_id = 1",
        ]
        return " AND ".join(list(conditions) + DEFAULT_TEST_QUERY_CONDITIONS)

    conds = " OR ".join(
        ["(group_id=268128807 AND group_id=268128807)" for i in range(NUM_CONDS)]
    )
    snql = """
    MATCH (events)
    SELECT group_id, goo(partition) AS issue_id,
            foo(zoo(offset)) AS offset
    WHERE {conditions}
    ORDER BY group_id ASC
    """.format(
        conditions=snql_conditions_with_default(f"({conds})")
    )
    parse_snql_query(snql, get_dataset("events"))
