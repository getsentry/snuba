from typing import Any, Sequence

import pytest
from snuba.clickhouse.columns import UUID, ColumnSet, String, UInt
from snuba.clickhouse.formatter.nodes import PaddingNode, SequenceNode, StringNode
from snuba.clickhouse.formatter.query import JoinFormatter, format_query
from snuba.clickhouse.query import Query
from snuba.query import LimitBy, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinModifier,
    JoinType,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.request.request_settings import HTTPRequestSettings

ERRORS_SCHEMA = ColumnSet(
    [
        ("event_id", UUID()),
        ("project_id", UInt(32)),
        ("message", String()),
        ("group_id", UInt(32)),
    ]
)
GROUPS_SCHEMA = ColumnSet(
    [
        ("id", UInt(32)),
        ("project_id", UInt(32)),
        ("group_id", UInt(32)),
        ("message", String()),
    ]
)
GROUPS_ASSIGNEE = ColumnSet([("id", UInt(32)), ("user", String())])

node_err = IndividualNode(alias="err", data_source=Table("errors_local", ERRORS_SCHEMA))
node_group = IndividualNode(
    alias="groups", data_source=Table("groupedmessage_local", GROUPS_SCHEMA)
)
node_assignee = IndividualNode(
    alias="assignee", data_source=Table("groupassignee_local", GROUPS_ASSIGNEE)
)

test_cases = [
    pytest.param(
        Query(
            Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column1", Column(None, None, "column1")),
                SelectedExpression("column2", Column(None, "table1", "column2")),
                SelectedExpression("column3", Column("al", None, "column3")),
            ],
            condition=binary_condition(
                "eq", lhs=Column("al", None, "column3"), rhs=Literal(None, "blabla"),
            ),
            groupby=[
                Column(None, None, "column1"),
                Column(None, "table1", "column2"),
                Column("al", None, "column3"),
                Column(None, None, "column4"),
            ],
            having=binary_condition(
                "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, 123),
            ),
            order_by=[
                OrderBy(OrderByDirection.ASC, Column(None, None, "column1")),
                OrderBy(OrderByDirection.DESC, Column(None, "table1", "column2")),
            ],
        ),
        [
            "SELECT column1, table1.column2, (column3 AS al)",
            ["FROM", "my_table"],
            "WHERE eq(al, 'blabla')",
            "GROUP BY column1, table1.column2, al, column4",
            "HAVING eq(column1, 123)",
            "ORDER BY column1 ASC, table1.column2 DESC",
        ],
        (
            "SELECT column1, table1.column2, (column3 AS al) "
            "FROM my_table "
            "WHERE eq(al, 'blabla') "
            "GROUP BY column1, table1.column2, al, column4 "
            "HAVING eq(column1, 123) "
            "ORDER BY column1 ASC, table1.column2 DESC"
        ),
        id="Simple query with aliases and multiple tables",
    ),
    pytest.param(
        Query(
            Table("my_table", ColumnSet([])),
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
                ),
            ],
            condition=binary_condition(
                "and",
                lhs=binary_condition(
                    "eq",
                    lhs=Column("al", None, "column3"),
                    rhs=Literal(None, "blabla"),
                ),
                rhs=binary_condition(
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
                ),
            ],
            order_by=[
                OrderBy(
                    OrderByDirection.ASC,
                    FunctionCall(None, "f", (Column(None, None, "column1"),)),
                )
            ],
        ),
        [
            "SELECT (doSomething(column1, table1.column2, (column3 AS al))(column1) AS my_complex_math)",
            ["FROM", "my_table"],
            "WHERE eq(al, 'blabla') AND neq(al, 'blabla')",
            "GROUP BY my_complex_math",
            "ORDER BY f(column1) ASC",
        ],
        (
            "SELECT (doSomething(column1, table1.column2, (column3 AS al))(column1) AS my_complex_math) "
            "FROM my_table "
            "WHERE eq(al, 'blabla') AND neq(al, 'blabla') "
            "GROUP BY my_complex_math "
            "ORDER BY f(column1) ASC"
        ),
        id="Query with complex functions",
    ),
    pytest.param(
        Query(
            Table("my_table", ColumnSet([])),
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
        [
            "SELECT (`field_##$$%` AS al1), (`t&^%$`.`f@!@` AS al2)",
            ["FROM", "my_table"],
            "GROUP BY al1, al2",
            "ORDER BY column1 ASC",
        ],
        (
            "SELECT (`field_##$$%` AS al1), (`t&^%$`.`f@!@` AS al2) "
            "FROM my_table "
            "GROUP BY al1, al2 "
            "ORDER BY column1 ASC"
        ),
        id="Query with escaping",
    ),
    pytest.param(
        Query(
            Table("my_table", ColumnSet([])),
            selected_columns=[
                SelectedExpression("al", Column("al", None, "column3")),
                SelectedExpression("al2", Column("al2", None, "column4")),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    BooleanFunctions.OR,
                    binary_condition(
                        ConditionFunctions.EQ,
                        lhs=Column("al", None, "column3"),
                        rhs=Literal(None, "blabla"),
                    ),
                    binary_condition(
                        ConditionFunctions.EQ,
                        lhs=Column("al2", None, "column4"),
                        rhs=Literal(None, "blabla2"),
                    ),
                ),
                binary_condition(
                    BooleanFunctions.OR,
                    binary_condition(
                        ConditionFunctions.EQ,
                        lhs=Column(None, None, "column5"),
                        rhs=Literal(None, "blabla3"),
                    ),
                    binary_condition(
                        ConditionFunctions.EQ,
                        lhs=Column(None, None, "column6"),
                        rhs=Literal(None, "blabla4"),
                    ),
                ),
            ),
        ),
        [
            "SELECT (column3 AS al), (column4 AS al2)",
            ["FROM", "my_table"],
            (
                "WHERE (equals(al, 'blabla') OR equals(al2, 'blabla2')) AND "
                "(equals(column5, 'blabla3') OR equals(column6, 'blabla4'))"
            ),
        ],
        (
            "SELECT (column3 AS al), (column4 AS al2) "
            "FROM my_table "
            "WHERE (equals(al, 'blabla') OR equals(al2, 'blabla2')) AND "
            "(equals(column5, 'blabla3') OR equals(column6, 'blabla4'))"
        ),
        id="query_complex_condition",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=Query(
                Table("my_table", ColumnSet([])),
                selected_columns=[
                    SelectedExpression("column1", Column(None, None, "column1")),
                    SelectedExpression(
                        "sub_average",
                        FunctionCall(
                            "sub_average", "avg", (Column(None, None, "column2"),)
                        ),
                    ),
                    SelectedExpression("column3", Column(None, None, "column3")),
                ],
                condition=binary_condition(
                    "eq",
                    lhs=Column("al", None, "column3"),
                    rhs=Literal(None, "blabla"),
                ),
                groupby=[Column(None, None, "column2")],
            ),
            selected_columns=[
                SelectedExpression(
                    "average",
                    FunctionCall(
                        "average", "avg", (Column(None, None, "sub_average"),)
                    ),
                ),
                SelectedExpression("alias", Column("alias", None, "column3")),
            ],
            groupby=[Column(None, None, "alias")],
        ),
        [
            "SELECT (avg(sub_average) AS average), (column3 AS alias)",
            [
                "FROM",
                [
                    "SELECT column1, (avg(column2) AS sub_average), column3",
                    ["FROM", "my_table"],
                    "WHERE eq((column3 AS al), 'blabla')",
                    "GROUP BY column2",
                ],
            ],
            "GROUP BY alias",
        ],
        (
            "SELECT (avg(sub_average) AS average), (column3 AS alias) "
            "FROM ("
            "SELECT column1, (avg(column2) AS sub_average), column3 "
            "FROM my_table "
            "WHERE eq((column3 AS al), 'blabla') "
            "GROUP BY column2"
            ") "
            "GROUP BY alias"
        ),
        id="Composite query",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=node_err,
                right_node=node_group,
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("err", "group_id"),
                        right=JoinConditionExpression("groups", "id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression("error_id", Column("error_id", "err", "event_id")),
                SelectedExpression("message", Column("message", "groups", "message")),
            ],
            condition=binary_condition(
                "eq", Column(None, "groups", "id"), Literal(None, 1)
            ),
        ),
        [
            "SELECT (err.event_id AS error_id), (groups.message AS message)",
            [
                "FROM",
                [
                    ["errors_local", "err"],
                    "INNER JOIN",
                    ["groupedmessage_local", "groups"],
                    "ON",
                    ["err.group_id=groups.id"],
                ],
            ],
            "WHERE eq(groups.id, 1)",
        ],
        (
            "SELECT (err.event_id AS error_id), (groups.message AS message) "
            "FROM errors_local err INNER JOIN groupedmessage_local groups "
            "ON err.group_id=groups.id "
            "WHERE eq(groups.id, 1)"
        ),
        id="Simple join",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=JoinClause(
                left_node=JoinClause(
                    left_node=IndividualNode(
                        alias="err",
                        data_source=Query(
                            from_clause=Table("errors_local", ERRORS_SCHEMA),
                            selected_columns=[
                                SelectedExpression(
                                    "error_id", Column("error_id", None, "event_id")
                                ),
                                SelectedExpression(
                                    "group_id", Column("group_id", None, "group_id")
                                ),
                            ],
                            condition=binary_condition(
                                "eq",
                                Column(None, None, "project_id"),
                                Literal(None, 1),
                            ),
                        ),
                    ),
                    right_node=IndividualNode(
                        alias="groups",
                        data_source=Query(
                            from_clause=Table("groupedmessage_local", GROUPS_SCHEMA),
                            selected_columns=[
                                SelectedExpression("id", Column("id", None, "id")),
                                SelectedExpression(
                                    "message", Column("message", None, "message")
                                ),
                            ],
                            condition=binary_condition(
                                "eq",
                                Column(None, None, "project_id"),
                                Literal(None, 1),
                            ),
                        ),
                    ),
                    keys=[
                        JoinCondition(
                            left=JoinConditionExpression("err", "group_id"),
                            right=JoinConditionExpression("groups", "id"),
                        )
                    ],
                    join_type=JoinType.INNER,
                ),
                right_node=IndividualNode(
                    alias="assignee",
                    data_source=Query(
                        from_clause=Table("groupassignee_local", GROUPS_ASSIGNEE),
                        selected_columns=[
                            SelectedExpression(
                                "group_id", Column("group_id", None, "group_id")
                            ),
                        ],
                        condition=binary_condition(
                            "eq", Column(None, None, "user"), Literal(None, "me"),
                        ),
                    ),
                ),
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("err", "group_id"),
                        right=JoinConditionExpression("assignee", "group_id"),
                    )
                ],
                join_type=JoinType.INNER,
            ),
            selected_columns=[
                SelectedExpression("group_id", Column("group_id", "err", "group_id")),
                SelectedExpression("events", FunctionCall("events", "count", tuple())),
            ],
            groupby=[Column(None, "groups", "id")],
        ),
        [
            "SELECT (err.group_id AS group_id), (count() AS events)",
            [
                "FROM",
                [
                    [
                        [
                            [
                                "SELECT (event_id AS error_id), group_id",
                                ["FROM", "errors_local"],
                                "WHERE eq(project_id, 1)",
                            ],
                            "err",
                        ],
                        "INNER JOIN",
                        [
                            [
                                "SELECT id, message",
                                ["FROM", "groupedmessage_local"],
                                "WHERE eq(project_id, 1)",
                            ],
                            "groups",
                        ],
                        "ON",
                        ["err.group_id=groups.id"],
                    ],
                    "INNER JOIN",
                    [
                        [
                            "SELECT group_id",
                            ["FROM", "groupassignee_local"],
                            "WHERE eq(user, 'me')",
                        ],
                        "assignee",
                    ],
                    "ON",
                    ["err.group_id=assignee.group_id"],
                ],
            ],
            "GROUP BY groups.id",
        ],
        (
            "SELECT (err.group_id AS group_id), (count() AS events) "
            "FROM "
            "(SELECT (event_id AS error_id), group_id FROM errors_local WHERE eq(project_id, 1)) err "
            "INNER JOIN "
            "(SELECT id, message FROM groupedmessage_local WHERE eq(project_id, 1)) groups "
            "ON err.group_id=groups.id "
            "INNER JOIN "
            "(SELECT group_id FROM groupassignee_local WHERE eq(user, 'me')) assignee "
            "ON err.group_id=assignee.group_id "
            "GROUP BY groups.id"
        ),
        id="Join of multiple subqueries",
    ),
]


@pytest.mark.parametrize("query, formatted_seq, formatted_str", test_cases)
def test_format_expressions(
    query: Query, formatted_seq: Sequence[Any], formatted_str: str
) -> None:
    request_settings = HTTPRequestSettings()
    clickhouse_query = format_query(query, request_settings)
    assert clickhouse_query.get_sql() == formatted_str
    assert clickhouse_query.structured() == formatted_seq


def test_format_clickhouse_specific_query() -> None:
    """
    Adds a few of the Clickhosue specific fields to the query.
    """

    query = Query(
        Table("my_table", ColumnSet([]), final=True, sampling_rate=0.1),
        selected_columns=[
            SelectedExpression("column1", Column(None, None, "column1")),
            SelectedExpression("column2", Column(None, "table1", "column2")),
        ],
        condition=binary_condition(
            "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, "blabla"),
        ),
        groupby=[Column(None, None, "column1"), Column(None, "table1", "column2")],
        having=binary_condition(
            "eq", lhs=Column(None, None, "column1"), rhs=Literal(None, 123),
        ),
        order_by=[OrderBy(OrderByDirection.ASC, Column(None, None, "column1"))],
        array_join=Column(None, None, "column1"),
        totals=True,
        limitby=LimitBy(10, Column(None, None, "environment")),
    )

    query.set_offset(50)
    query.set_limit(100)

    request_settings = HTTPRequestSettings()
    clickhouse_query = format_query(query, request_settings)

    expected = (
        "SELECT column1, table1.column2 "
        "FROM my_table FINAL SAMPLE 0.1 "
        "ARRAY JOIN column1 "
        "WHERE eq(column1, 'blabla') "
        "GROUP BY column1, table1.column2 WITH TOTALS "
        "HAVING eq(column1, 123) "
        "ORDER BY column1 ASC "
        "LIMIT 10 BY environment "
        "LIMIT 100 OFFSET 50"
    )

    assert clickhouse_query.get_sql() == expected


TEST_JOIN = [
    pytest.param(
        JoinClause(
            left_node=node_err,
            right_node=node_group,
            keys=[
                JoinCondition(
                    left=JoinConditionExpression("err", "group_id"),
                    right=JoinConditionExpression("groups", "id"),
                ),
                JoinCondition(
                    left=JoinConditionExpression("err", "project_id"),
                    right=JoinConditionExpression("groups", "project_id"),
                ),
            ],
            join_type=JoinType.INNER,
            join_modifier=JoinModifier.SEMI,
        ),
        SequenceNode(
            [
                PaddingNode(None, StringNode("errors_local"), "err"),
                StringNode("SEMI INNER JOIN"),
                PaddingNode(None, StringNode("groupedmessage_local"), "groups"),
                StringNode("ON"),
                SequenceNode(
                    [
                        StringNode("err.group_id=groups.id"),
                        StringNode("err.project_id=groups.project_id"),
                    ],
                    " AND ",
                ),
            ]
        ),
        (
            "errors_local err SEMI INNER JOIN groupedmessage_local groups "
            "ON err.group_id=groups.id AND err.project_id=groups.project_id"
        ),
        id="Simple join",
    ),
    pytest.param(
        JoinClause(
            left_node=JoinClause(
                left_node=node_err,
                right_node=node_group,
                keys=[
                    JoinCondition(
                        left=JoinConditionExpression("err", "group_id"),
                        right=JoinConditionExpression("groups", "id"),
                    )
                ],
                join_type=JoinType.INNER,
                join_modifier=JoinModifier.SEMI,
            ),
            right_node=node_assignee,
            keys=[
                JoinCondition(
                    left=JoinConditionExpression("err", "group_id"),
                    right=JoinConditionExpression("assignee", "id"),
                )
            ],
            join_type=JoinType.INNER,
        ),
        SequenceNode(
            [
                SequenceNode(
                    [
                        PaddingNode(None, StringNode("errors_local"), "err"),
                        StringNode("SEMI INNER JOIN"),
                        PaddingNode(None, StringNode("groupedmessage_local"), "groups"),
                        StringNode("ON"),
                        SequenceNode([StringNode("err.group_id=groups.id")], " AND "),
                    ]
                ),
                StringNode("INNER JOIN"),
                PaddingNode(None, StringNode("groupassignee_local"), "assignee"),
                StringNode("ON"),
                SequenceNode([StringNode("err.group_id=assignee.id")], " AND "),
            ]
        ),
        (
            "errors_local err SEMI INNER JOIN groupedmessage_local groups "
            "ON err.group_id=groups.id INNER JOIN groupassignee_local assignee "
            "ON err.group_id=assignee.id"
        ),
        id="Complex join",
    ),
]


@pytest.mark.parametrize("clause, formatted_seq, formatted_str", TEST_JOIN)
def test_join_format(
    clause: JoinClause[Table], formatted_seq: SequenceNode, formatted_str: str
) -> None:
    assert str(clause.accept(JoinFormatter())) == formatted_str
    assert clause.accept(JoinFormatter()) == formatted_seq
