from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.query import format_query
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, Literal


def test_format_clickhouse_specific_query() -> None:
    """
    Adds a few of the Clickhosue specific fields to the query.
    """

    query = ClickhouseQuery(
        Table(
            "my_table",
            ColumnSet([]),
            final=True,
            sampling_rate=0.1,
            storage_key=StorageKey("dontmatter"),
        ),
        selected_columns=[
            SelectedExpression("column1", Column(None, None, "column1")),
            SelectedExpression("column2", Column(None, "table1", "column2")),
        ],
        condition=binary_condition(
            "eq",
            lhs=Column(None, None, "column1"),
            rhs=Literal(None, "blabla"),
        ),
        groupby=[Column(None, None, "column1"), Column(None, "table1", "column2")],
        having=binary_condition(
            "eq",
            lhs=Column(None, None, "column1"),
            rhs=Literal(None, 123),
        ),
        order_by=[OrderBy(OrderByDirection.ASC, Column(None, None, "column1"))],
        array_join=[Column(None, None, "column1")],
        totals=True,
        limitby=LimitBy(10, [Column(None, None, "environment")]),
    )

    query.set_offset(50)
    query.set_limit(100)

    clickhouse_query = format_query(query)

    expected = [
        "SELECT column1, table1.column2",
        ["FROM", "my_table FINAL SAMPLE 0.1"],
        "ARRAY JOIN column1",
        "WHERE eq(column1, 'blabla')",
        "GROUP BY column1, table1.column2 WITH TOTALS",
        "HAVING eq(column1, 123)",
        "ORDER BY column1 ASC",
        "LIMIT 10 BY environment",
        "LIMIT 100 OFFSET 50",
    ]

    assert clickhouse_query.structured() == expected
