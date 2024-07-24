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
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import NestedColumn, and_cond, column, in_cond, literal
from snuba.query.expressions import Column, FunctionCall
from snuba.query.logical import Query
from snuba.query.mql.parser_supported_join import parse_mql_query_new
from snuba.query.snql.parser import parse_snql_query

tags = NestedColumn("tags")
tags_raw = NestedColumn("tags_raw")
from_distributions = QueryEntity(
    EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    get_entity(EntityKey.GENERIC_METRICS_DISTRIBUTIONS).get_data_model(),
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
        totals=True,
        limit=1000,
    )
    actual = parse_mql_query_new(mql, context, get_dataset("generic_metrics"))
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
        totals=True,
    )
    actual = parse_mql_query_new(mql, context, get_dataset("generic_metrics"))
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
        totals=True,
    )
    actual = parse_mql_query_new(mql, context, get_dataset("generic_metrics"))
    eq, reason = actual.equals(expected)
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
    parse_mql_query_new(mql, context, get_dataset("generic_metrics"))

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
