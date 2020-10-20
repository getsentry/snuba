from typing import List, Tuple

import pytest
from snuba.clickhouse.astquery import AstSqlQuery
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.schemas.tables import TableSource
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.request.request_settings import HTTPRequestSettings

test_cases = [
    pytest.param(
        # Simple query with aliases and multiple tables
        ClickhouseQuery(
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column(None, None, "column1")),
                SelectedExpression("column2", Column(None, "table1", "column2")),
                SelectedExpression("column3", Column("al", None, "column3")),
            ],
            condition=binary_condition(
                None,
                "eq",
                lhs=Column("al", None, "column3"),
                rhs=Literal(None, "blabla"),
            ),
            groupby=[
                Column(None, None, "column1"),
                Column(None, "table1", "column2"),
                Column("al", None, "column3"),
                Column(None, None, "column4"),
            ],
            having=binary_condition(
                None, "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, 123),
            ),
            order_by=[
                OrderBy(OrderByDirection.ASC, Column(None, None, "column1")),
                OrderBy(OrderByDirection.DESC, Column(None, "table1", "column2")),
            ],
        ),
        {
            "from": "FROM my_table",
            "group": "GROUP BY (column1, table1.column2, al, column4)",
            "having": "HAVING eq(column1, 123)",
            "order": "ORDER BY column1 ASC, table1.column2 DESC",
            "select": "SELECT column1, table1.column2, (column3 AS al)",
            "where": "WHERE eq(al, 'blabla')",
        },
        id="Query_with_aliases",
    ),
    pytest.param(
        # Query with complex functions
        ClickhouseQuery(
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "my_complex_math",
                    CurriedFunctionCall(
                        "my_complex_math",
                        FunctionCall(
                            None,
                            "doSomething",
                            (
                                Column(None, None, "column1"),
                                Column(None, "table1", "column2"),
                                Column("al", None, "column3"),
                            ),
                        ),
                        (Column(None, None, "column1"),),
                    ),
                )
            ],
            condition=binary_condition(
                None,
                "and",
                lhs=binary_condition(
                    None,
                    "eq",
                    lhs=Column("al", None, "column3"),
                    rhs=Literal(None, "blabla"),
                ),
                rhs=binary_condition(
                    None,
                    "neq",  # yes, not very smart
                    lhs=Column("al", None, "column3"),
                    rhs=Literal(None, "blabla"),
                ),
            ),
            groupby=[
                CurriedFunctionCall(
                    "my_complex_math",
                    FunctionCall(
                        None,
                        "doSomething",
                        (
                            Column(None, None, "column1"),
                            Column(None, "table1", "column2"),
                            Column("al", None, "column3"),
                        ),
                    ),
                    (Column(None, None, "column1"),),
                )
            ],
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(None, "f", (Column(None, None, "column1"),)),
                )
            ],
        ),
        {
            "from": "FROM my_table",
            "group": "GROUP BY (my_complex_math)",
            "order": "ORDER BY f(column1) ASC",
            "select": "SELECT (doSomething(column1, table1.column2, (column3 AS "
            "al))(column1) AS my_complex_math)",
            "where": "WHERE eq(al, 'blabla') AND neq(al, 'blabla')",
        },
        id="complex_functions",
    ),
    pytest.param(
        # Query with escaping
        ClickhouseQuery(
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("field_##$$%", Column("al1", None, "field_##$$%")),
                SelectedExpression("f@!@", Column("al2", "t&^%$", "f@!@")),
            ],
            groupby=[
                Column("al1", None, "field_##$$%"),
                Column("al2", "t&^%$", "f@!@"),
            ],
            order_by=[OrderBy(OrderByDirection.ASC, Column(None, None, "column1"))],
        ),
        {
            "from": "FROM my_table",
            "group": "GROUP BY (al1, al2)",
            "order": "ORDER BY column1 ASC",
            "select": "SELECT (`field_##$$%` AS al1), (`t&^%$`.`f@!@` AS al2)",
        },
        id="query_with_escaping",
    ),
    pytest.param(
        ClickhouseQuery(
            TableSource("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("al", Column("al", None, "column3")),
                SelectedExpression("al2", Column("al2", None, "column4")),
            ],
            condition=binary_condition(
                None,
                BooleanFunctions.AND,
                binary_condition(
                    None,
                    BooleanFunctions.OR,
                    binary_condition(
                        None,
                        ConditionFunctions.EQ,
                        lhs=Column("al", None, "column3"),
                        rhs=Literal(None, "blabla"),
                    ),
                    binary_condition(
                        None,
                        ConditionFunctions.EQ,
                        lhs=Column("al2", None, "column4"),
                        rhs=Literal(None, "blabla2"),
                    ),
                ),
                binary_condition(
                    None,
                    BooleanFunctions.OR,
                    binary_condition(
                        None,
                        ConditionFunctions.EQ,
                        lhs=Column(None, None, "column5"),
                        rhs=Literal(None, "blabla3"),
                    ),
                    binary_condition(
                        None,
                        ConditionFunctions.EQ,
                        lhs=Column(None, None, "column6"),
                        rhs=Literal(None, "blabla4"),
                    ),
                ),
            ),
        ),
        {
            "from": "FROM my_table",
            "select": "SELECT (column3 AS al), (column4 AS al2)",
            "where": (
                "WHERE (equals(al, 'blabla') OR equals(al2, 'blabla2')) AND "
                "(equals(column5, 'blabla3') OR equals(column6, 'blabla4'))"
            ),
        },
        id="query_complex_condition",
    ),
]


@pytest.mark.parametrize("query, data", test_cases)
def test_format_expressions(
    query: ClickhouseQuery, data: List[Tuple[str, str]]
) -> None:
    request_settings = HTTPRequestSettings()
    clickhouse_query = AstSqlQuery(query, request_settings)
    assert clickhouse_query.sql_data() == data


def test_format_clickhouse_specific_query() -> None:
    """
    Adds a few of the Clickhosue specific fields to the query.
    """

    query = ClickhouseQuery(
        TableSource("my_table", ColumnSet([])),
        selected_columns=[
            SelectedExpression("column1", Column(None, None, "column1")),
            SelectedExpression("column2", Column(None, "table1", "column2")),
        ],
        condition=binary_condition(
            None, "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, "blabla"),
        ),
        groupby=[Column(None, None, "column1"), Column(None, "table1", "column2")],
        having=binary_condition(
            None, "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, 123),
        ),
        order_by=[OrderBy(OrderByDirection.ASC, Column(None, None, "column1"))],
        array_join=Column(None, None, "column1"),
        sample=0.1,
        totals=True,
        limitby=(10, "environment"),
    )

    query.set_final(True)
    query.set_offset(50)
    query.set_limit(100)

    request_settings = HTTPRequestSettings()
    clickhouse_query = AstSqlQuery(query, request_settings)

    expected = {
        "from": "FROM my_table FINAL SAMPLE 0.1",
        "group": "GROUP BY (column1, table1.column2) WITH TOTALS",
        "having": "HAVING eq(column1, 123)",
        "array_join": "ARRAY JOIN column1",
        "limit": "LIMIT 100 OFFSET 50",
        "limitby": "LIMIT 10 BY environment",
        "order": "ORDER BY column1 ASC",
        "select": "SELECT column1, table1.column2",
        "where": "WHERE eq(column1, 'blabla')",
    }

    assert clickhouse_query.sql_data() == expected
