from datetime import datetime

import pytest

from snuba.clickhouse.formatter.expression import (
    ClickhouseExpressionFormatter,
    ExpressionFormatterAnonymized,
)
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.parsing import ParsingContext

test_expressions = [
    (Literal(None, "test"), "'test'", "'$S'"),  # String literal
    (Literal(None, 123), "123", "-1337"),  # INT literal
    (
        Literal("something", 123),
        "(123 AS something)",
        "-1337",
    ),  # INT literal with alias
    (Literal(None, 123.321), "123.321", "-1337"),  # FLOAT literal
    (Literal(None, None), "NULL", "NULL"),  # NULL
    (
        Literal("not_null", None),
        "(NULL AS not_null)",
        "(NULL AS not_null)",
    ),  # NULL with alias
    (Literal(None, True), "true", "true"),
    (Literal(None, False), "false", "false"),
    (
        Literal(None, datetime(2020, 4, 20, 16, 20)),
        "toDateTime('2020-04-20T16:20:00', 'Universal')",
        "toDateTime('2020-04-20T16:20:00', 'Universal')",
    ),
    (
        Literal(None, datetime(2020, 4, 20, 16, 20).date()),
        "toDate('2020-04-20', 'Universal')",
        "toDate('2020-04-20', 'Universal')",
    ),
    (
        Column(None, "table1", "column1"),
        "table1.column1",
        "table1.column1",
    ),  # Basic Column no alias
    (
        Column("table1.column1", "table1", "column1"),
        "table1.column1",
        "table1.column1",
    ),  # Declutter aliases - column name is the same as the alias. Do not alias
    (Column(None, None, "column1"), "column1", "column1"),  # Basic Column with no table
    (
        Column("alias", "table1", "column1"),
        "(table1.column1 AS alias)",
        "(table1.column1 AS alias)",
    ),  # Column with table and alias
    (
        FunctionCall(
            None,
            "f1",
            (
                Column(None, "table1", "tags"),
                Column(None, "table1", "param2"),
                Literal(None, None),
                Literal(None, "test_string"),
            ),
        ),
        "f1(table1.tags, table1.param2, NULL, 'test_string')",
        "f1(table1.tags, table1.param2, NULL, '$S')",
    ),  # Simple function call with columns and literals
    (
        FunctionCall(
            "alias",
            "f1",
            (Column(None, "table1", "param1"), Column("alias1", "table1", "param2")),
        ),
        "(f1(table1.param1, (table1.param2 AS alias1)) AS alias)",
        "(f1(table1.param1, (table1.param2 AS alias1)) AS alias)",
    ),  # Function with alias
    (
        FunctionCall(
            None,
            "f1",
            (
                FunctionCall(None, "f2", (Column(None, "table1", "param1"),)),
                FunctionCall(None, "f3", (Column(None, "table1", "param2"),)),
            ),
        ),
        "f1(f2(table1.param1), f3(table1.param2))",
        "f1(f2(table1.param1), f3(table1.param2))",
    ),  # Hierarchical function call
    (
        FunctionCall(
            None,
            "f1",
            (
                FunctionCall("al1", "f2", (Column(None, "table1", "param1"),)),
                FunctionCall("al2", "f3", (Column(None, "table1", "param2"),)),
            ),
        ),
        "f1((f2(table1.param1) AS al1), (f3(table1.param2) AS al2))",
        "f1((f2(table1.param1) AS al1), (f3(table1.param2) AS al2))",
    ),  # Hierarchical function call with aliases
    (
        CurriedFunctionCall(
            None,
            FunctionCall(None, "f0", (Column(None, "table1", "param1"),)),
            (
                FunctionCall(None, "f1", (Column(None, "table1", "param2"),)),
                Column(None, "table1", "param3"),
            ),
        ),
        "f0(table1.param1)(f1(table1.param2), table1.param3)",
        "f0(table1.param1)(f1(table1.param2), table1.param3)",
    ),  # Curried function call with hierarchy
    (
        FunctionCall(
            None,
            "arrayExists",
            (
                Lambda(
                    None,
                    ("x", "y"),
                    FunctionCall(
                        None, "testFunc", (Argument(None, "x"), Argument(None, "y"))
                    ),
                ),
                Column(None, None, "test"),
            ),
        ),
        "arrayExists(x, y -> testFunc(x, y), test)",
        "arrayExists(x, y -> testFunc(x, y), test)",
    ),  # Lambda expression
    (
        FunctionCall("alias", "array", (Literal(None, 1), Literal(None, 2))),
        "([1, 2] AS alias)",
        "([-1337, -1337] AS alias)",
    ),  # Formatting an array as [...]
    (
        binary_condition(
            BooleanFunctions.AND,
            binary_condition(
                BooleanFunctions.OR,
                binary_condition(
                    BooleanFunctions.OR,
                    binary_condition(
                        BooleanFunctions.AND,
                        binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "c1"),
                            Literal(None, 1),
                        ),
                        binary_condition(
                            ConditionFunctions.EQ,
                            Column(None, None, "c2"),
                            Literal(None, 2),
                        ),
                    ),
                    binary_condition(
                        ConditionFunctions.EQ,
                        Column(None, None, "c3"),
                        Literal(None, 3),
                    ),
                ),
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "c4"),
                    Literal(None, 4),
                ),
            ),
            binary_condition(
                ConditionFunctions.EQ, Column(None, None, "c5"), Literal(None, 5)
            ),
        ),
        "(equals(c1, 1) AND equals(c2, 2) OR equals(c3, 3) OR equals(c4, 4)) AND equals(c5, 5)",
        "(equals(c1, -1337) AND equals(c2, -1337) OR equals(c3, -1337) OR equals(c4, -1337)) AND equals(c5, -1337)",
    ),  # Formatting infix expressions
    (
        FunctionCall(
            "_snuba_tags[some_pii]", "f0", (Column(None, "table1", "param1"),)
        ),
        "(f0(table1.param1) AS `_snuba_tags[some_pii]`)",
        "(f0(table1.param1) AS `_snuba_tags[$A]`)",
    ),
    (
        FunctionCall(
            "_snuba_tags[some_pii][some_more_pii]",
            "f0",
            (Column(None, "table1", "param1"),),
        ),
        "(f0(table1.param1) AS `_snuba_tags[some_pii][some_more_pii]`)",
        "(f0(table1.param1) AS `_snuba_tags[$A][$A]`)",
    ),
    (
        FunctionCall("snubatagssomepii", "f0", (Column(None, "table1", "param1"),)),
        "(f0(table1.param1) AS snubatagssomepii)",
        "(f0(table1.param1) AS snubatagssomepii)",
    ),
    (
        FunctionCall(
            "some_tuple",
            "tuple",
            (Column(None, "table1", "param1"), Column(None, "table1", "param2")),
        ),
        "((table1.param1, table1.param2) AS some_tuple)",
        "((table1.param1, table1.param2) AS some_tuple)",
    ),
    (
        FunctionCall(
            "single_tuple",
            "tuple",
            (Column(None, "table1", "param1"),),
        ),
        "(tuple(table1.param1) AS single_tuple)",
        "(tuple(table1.param1) AS single_tuple)",
    ),
]


@pytest.mark.parametrize(
    "expression, expected_clickhouse, expected_anonymized", test_expressions
)
def test_format_expressions(
    expression: Expression, expected_clickhouse: str, expected_anonymized: str
) -> None:
    visitor = ClickhouseExpressionFormatter()
    anonymized_visitor = ExpressionFormatterAnonymized()
    assert expression.accept(visitor) == expected_clickhouse
    assert expression.accept(anonymized_visitor) == expected_anonymized


def test_aliases() -> None:
    # No context
    col1 = Column("al1", "table1", "column1")
    col2 = Column("al1", "table1", "column1")

    assert col1.accept(ClickhouseExpressionFormatter()) == "(table1.column1 AS al1)"
    assert col2.accept(ClickhouseExpressionFormatter()) == "(table1.column1 AS al1)"

    # With Context
    pc = ParsingContext()
    assert col1.accept(ClickhouseExpressionFormatter(pc)) == "(table1.column1 AS al1)"
    assert col2.accept(ClickhouseExpressionFormatter(pc)) == "al1"

    # Hierarchical expression inherits parsing context and applies aliases
    f = FunctionCall(
        None,
        "f1",
        (
            FunctionCall("tag[something]", "tag", (Column(None, "table1", "column1"),)),
            FunctionCall("tag[something]", "tag", (Column(None, "table1", "column1"),)),
            FunctionCall("tag[something]", "tag", (Column(None, "table1", "column1"),)),
        ),
    )

    expected = "f1((tag(table1.column1) AS `tag[something]`), `tag[something]`, `tag[something]`)"
    assert f.accept(ClickhouseExpressionFormatter()) == expected


test_escaped = [
    (
        Column(None, "table.something", "tags.values"),
        "table.something.`tags.values`",
    ),  # Columns with dot are not escaped
    (
        Column(None, "weird_!@#$%^^&*_table", "tags[something]"),
        "`weird_!@#$%^^&*_table`.`tags[something]`",
    ),  # Somebody thought that table name was a good idea.
    (
        Column("alias.cannot.have.dot", "table", "columns.can"),
        "(table.`columns.can` AS `alias.cannot.have.dot`)",
    ),  # Escaping is different between columns and aliases
    (
        FunctionCall(None, "f*&^%$#unction", (Column(None, "table", "column"),)),
        "`f*&^%$#unction`(table.column)",
    ),  # Function names can be escaped. Hopefully it will never happen
    (
        Column(
            alias="_snuba_group_id",
            table_name="groups",
            column_name="_snuba_groups.group_id",
        ),
        "(groups.`_snuba_groups.group_id` AS _snuba_group_id)",
    ),  # Aliased column names with dot are escaped if there's an exising table name
]


@pytest.mark.parametrize("expression, expected", test_escaped)
def test_escaping(expression: Expression, expected: str) -> None:
    visitor = ClickhouseExpressionFormatter()
    assert expression.accept(visitor) == expected
