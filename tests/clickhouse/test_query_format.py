import pytest

from snuba.clickhouse.astquery import AstClickhouseQuery
from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.query import OrderBy, OrderByDirection, Query
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
)
from snuba.query.conditions import binary_condition
from snuba.request.request_settings import HTTPRequestSettings

test_cases = [
    (
        # Simple query with aliases and multiple tables
        Query(
            {},
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                Column(None, "column1", None),
                Column(None, "column2", "table1"),
                Column("al", "column3", None),
            ],
            condition=binary_condition(
                None,
                "eq",
                lhs=Column("al", "column3", None),
                rhs=Literal(None, "blabla"),
            ),
            groupby=[
                Column(None, "column1", None),
                Column(None, "column2", "table1"),
                Column("al", "column3", None),
                Column(None, "column4", None),
            ],
            having=binary_condition(
                None, "eq", lhs=Column(None, "column1", None), rhs=Literal(None, 123),
            ),
            order_by=[
                OrderBy(OrderByDirection.ASC, Column(None, "column1", None)),
                OrderBy(OrderByDirection.DESC, Column(None, "column2", "table1")),
            ],
        ),
        (
            "SELECT column1, table1.column2, (column3 AS al) "
            "FROM my_table "
            "WHERE eq(al, 'blabla') "
            "GROUP BY (column1, table1.column2, al, column4) "
            "HAVING eq(column1, 123) "
            "ORDER BY column1 ASC, table1.column2 DESC"
        ),
    ),
    (
        # Query with complex functions
        Query(
            {},
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                CurriedFunctionCall(
                    "my_complex_math",
                    FunctionCall(
                        None,
                        "doSomething",
                        [
                            Column(None, "column1", None),
                            Column(None, "column2", "table1"),
                            Column("al", "column3", None),
                        ],
                    ),
                    [Column(None, "column1", None)],
                )
            ],
            condition=binary_condition(
                None,
                "and",
                lhs=binary_condition(
                    None,
                    "eq",
                    lhs=Column("al", "column3", None),
                    rhs=Literal(None, "blabla"),
                ),
                rhs=binary_condition(
                    None,
                    "neq",  # yes, not very smart
                    lhs=Column("al", "column3", None),
                    rhs=Literal(None, "blabla"),
                ),
            ),
            groupby=[
                CurriedFunctionCall(
                    "my_complex_math",
                    FunctionCall(
                        None,
                        "doSomething",
                        [
                            Column(None, "column1", None),
                            Column(None, "column2", "table1"),
                            Column("al", "column3", None),
                        ],
                    ),
                    [Column(None, "column1", None)],
                )
            ],
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(None, "f", [Column(None, "column1", None)]),
                )
            ],
        ),
        (
            "SELECT (doSomething(column1, table1.column2, (column3 AS al))(column1) AS my_complex_math) "
            "FROM my_table "
            "WHERE and(eq(al, 'blabla'), neq(al, 'blabla')) "
            "GROUP BY (my_complex_math) "
            "ORDER BY f(column1) ASC"
        ),
    ),
    (
        # Query with escaping
        Query(
            {},
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                Column("al1", "field_##$$%", None),
                Column("al2", "f@!@", "t&^%$"),
            ],
            groupby=[
                Column("al1", "field_##$$%", None),
                Column("al2", "f@!@", "t&^%$"),
            ],
            order_by=[OrderBy(OrderByDirection.ASC, Column(None, "column1", None))],
        ),
        (
            "SELECT (`field_##$$%` AS al1), (`t&^%$`.`f@!@` AS al2) "
            "FROM my_table "
            "GROUP BY (al1, al2) "
            "ORDER BY column1 ASC"
        ),
    ),
]


@pytest.mark.parametrize("query, formatted", test_cases)
def test_format_expressions(query: Query, formatted: str) -> None:
    request_settings = HTTPRequestSettings()
    clickhouse_query = AstClickhouseQuery(query, request_settings)
    assert clickhouse_query.format_sql() == formatted


def test_format_clickhouse_specific_query() -> None:
    """
    Adds a few of the Clickhosue specific fields to the query.
    """

    query = Query(
        {"sample": 0.1, "totals": True, "limitby": (10, "environment")},
        TableSource("my_table", ColumnSet([])),
        selected_columns=[
            Column(None, "column1", None),
            Column(None, "column2", "table1"),
        ],
        condition=binary_condition(
            None, "eq", lhs=Column(None, "column1", None), rhs=Literal(None, "blabla"),
        ),
        groupby=[Column(None, "column1", None), Column(None, "column2", "table1")],
        having=binary_condition(
            None, "eq", lhs=Column(None, "column1", None), rhs=Literal(None, 123),
        ),
        order_by=[OrderBy(OrderByDirection.ASC, Column(None, "column1", None))],
        array_join=Column(None, "column1", None),
    )

    query.set_final(True)
    query.set_offset(50)
    query.set_limit(100)

    request_settings = HTTPRequestSettings()
    clickhouse_query = AstClickhouseQuery(query, request_settings)

    expected = (
        "SELECT column1, table1.column2 "
        "FROM my_table FINAL SAMPLE 0.1 "
        "ARRAY JOIN column1 "
        "WHERE eq(column1, 'blabla') "
        "GROUP BY (column1, table1.column2) WITH TOTALS "
        "HAVING eq(column1, 123) "
        "ORDER BY column1 ASC "
        "LIMIT 10 BY environment "
        "LIMIT 100 OFFSET 50"
    )

    assert clickhouse_query.format_sql() == expected
