import pytest

from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
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
    (Literal(None, "test"), "'test'"),  # String literal
    (Literal(None, 123), "123",),  # INT literal
    (Literal("something", 123), "(123 AS something)",),  # INT literal with alias
    (Literal(None, 123.321), "123.321",),  # FLOAT literal
    (Literal(None, None), "NULL",),  # NULL
    (Literal("not_null", None), "(NULL AS not_null)",),  # NULL with alias
    (Literal(None, True), "true",),  # True
    (Literal(None, False), "false",),  # False
    (Column(None, "table1", "column1"), "table1.column1"),  # Basic Column no alias
    (
        Column("table1.column1", "table1", "column1"),
        "table1.column1",
    ),  # Declutter aliases - column name is the same as the alias. Do not alias
    (Column(None, None, "column1"), "column1"),  # Basic Column with no table
    (
        Column("alias", "table1", "column1"),
        "(table1.column1 AS alias)",
    ),  # Column with table and alias
    (
        Column("alias", "table1", "column1"),
        "(table1.column1 AS alias)",
    ),  # Column with table, alias and path
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
    ),  # Simple function call with columns and literals
    (
        FunctionCall(
            "alias",
            "f1",
            (Column(None, "table1", "param1"), Column("alias1", "table1", "param2")),
        ),
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
        "arrayExists((x, y -> testFunc(x, y)), test)",
    ),  # Lambda expression
    (
        FunctionCall("alias", "array", (Literal(None, 1), Literal(None, 2))),
        "([1, 2] AS alias)",
    ),  # Formatting an array as [...]
    (
        binary_condition(
            None,
            BooleanFunctions.AND,
            binary_condition(
                None,
                BooleanFunctions.OR,
                binary_condition(
                    None,
                    BooleanFunctions.OR,
                    binary_condition(
                        None,
                        BooleanFunctions.AND,
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            Column(None, None, "c1"),
                            Literal(None, 1),
                        ),
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            Column(None, None, "c2"),
                            Literal(None, 2),
                        ),
                    ),
                    binary_condition(
                        None,
                        ConditionFunctions.EQ,
                        Column(None, None, "c3"),
                        Literal(None, 3),
                    ),
                ),
                binary_condition(
                    None,
                    ConditionFunctions.EQ,
                    Column(None, None, "c4"),
                    Literal(None, 4),
                ),
            ),
            binary_condition(
                None, ConditionFunctions.EQ, Column(None, None, "c5"), Literal(None, 5)
            ),
        ),
        "(equals(c1, 1) AND equals(c2, 2) OR equals(c3, 3) OR equals(c4, 4)) AND equals(c5, 5)",
    ),  # Formatting infix expressions
]


@pytest.mark.parametrize("expression, expected", test_expressions)
def test_format_expressions(expression: Expression, expected: str) -> None:
    visitor = ClickhouseExpressionFormatter()
    assert expression.accept(visitor) == expected


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
        "table.something.tags.values",
    ),  # Columns with dot are not escaped
    (
        Column(None, "weird_!@#$%^^&*_table", "tags[something]"),
        "`weird_!@#$%^^&*_table`.`tags[something]`",
    ),  # Somebody thought that table name was a good idea.
    (
        Column("alias.cannot.have.dot", "table", "columns.can"),
        "(table.columns.can AS `alias.cannot.have.dot`)",
    ),  # Escaping is different between columns and aliases
    (
        FunctionCall(None, "f*&^%$#unction", (Column(None, "table", "column"),)),
        "`f*&^%$#unction`(table.column)",
    ),  # Function names can be escaped. Hopefully it will never happen
]


@pytest.mark.parametrize("expression, expected", test_escaped)
def test_escaping(expression: Expression, expected: str) -> None:
    visitor = ClickhouseExpressionFormatter()
    assert expression.accept(visitor) == expected
