import uuid
from dataclasses import replace
from datetime import datetime
from typing import Set

import pytest

from snuba.query.expressions import (
    ArbitrarySQL,
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


def test_iterate() -> None:
    """
    Test iteration over a subtree. The subtree is a function call in the form
    f2(c3, f1(c1, c2))
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall(None, "f1", (column1, column2))

    column3 = Column(None, "t1", "c2")
    column4 = Column(None, "t1", "c3")
    literal = Literal(None, "blablabla")
    function_2i = FunctionCall(None, "f2", (column3, function_1, literal))
    function_2 = CurriedFunctionCall(None, function_2i, (column4,))

    expected = [
        column3,
        column1,
        column2,
        function_1,
        literal,
        function_2i,
        column4,
        function_2,
    ]
    assert list(function_2) == expected


def test_aliased_cols() -> None:
    """
    Test iteration whan columns have aliases. This is the expression
    f2(t1.c2, f1(t1.c1, t1.c2 as a2)) as af1
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column("a2", "t1", "c2")
    function_1 = FunctionCall(None, "f1", (column1, column2))
    column3 = Column(None, "t1", "c2")
    function_2 = FunctionCall("af1", "f2", (column3, function_1))

    expected = [column3, column1, column2, function_1, function_2]
    assert list(function_2) == expected


def test_mapping_column_list() -> None:
    """
    Perform a simple mapping over a series of expressions.
    """

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c2":
            return FunctionCall(None, "f", (e,))
        return e

    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t2", "c2")
    column3 = Column(None, "t3", "c3")
    selected_cols = [column1, column2, column3]
    new_selected_cols = list(map(replace_col, selected_cols))

    assert new_selected_cols[0] == column1
    assert new_selected_cols[2] == column3
    f = new_selected_cols[1]
    assert isinstance(f, FunctionCall)
    assert f.function_name == "f"
    assert f.parameters == (column2,)


def test_add_alias() -> None:
    """
    Adds an alias to a column referenced in a function
    f(t1.c1) -> f(t1.c1 as a)
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column("a", "t1", "c1")

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c1":
            return column2
        return e

    f = FunctionCall(None, "f", (column1,))

    f2 = f.transform(replace_expr)
    expected = [column2, FunctionCall(None, "f", (column2,))]
    assert list(f2) == expected


def test_mapping_complex_expression() -> None:
    """
    Maps over an Expression container:
    f0(t1.c1, fB(f())) -> f0(t1.c1, fB(f(f() as a)))
    """

    f5 = FunctionCall("a", "f", ())
    f4 = FunctionCall(None, "f", (f5,))
    f3 = FunctionCall(None, "f", ())

    def replace_expr(e: Expression) -> Expression:
        if isinstance(e, FunctionCall) and e.function_name == "f":
            return f4
        return e

    c1 = Column(None, "t1", "c1")
    f2 = FunctionCall(None, "fB", (f3,))
    f1: Expression = FunctionCall(None, "f0", (c1, f2))

    # Only the external function is going to be replaced since, when map returns a new
    # column, we expect the func to have takern care of its own children.
    f1 = f1.transform(replace_expr)
    iterate = list(f1)
    expected = [
        c1,
        f5,
        f4,
        FunctionCall(None, "fB", (f4,)),
        FunctionCall(None, "f0", (c1, FunctionCall(None, "fB", (f4,)))),
    ]

    assert iterate == expected


def test_mapping_curried_function() -> None:
    c1 = Column(None, "t1", "c1")
    f1 = FunctionCall(None, "f1", (c1,))
    c2 = Column(None, "t1", "c1")
    f2 = CurriedFunctionCall(None, f1, (c2,))

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c1":
            return Column(None, "t1", "c2")
        return e

    f3 = f2.transform(replace_col)

    replaced_col = Column(None, "t1", "c2")
    replaced_function = FunctionCall(None, "f1", (replaced_col,))
    expected = [
        replaced_col,
        replaced_function,
        replaced_col,
        CurriedFunctionCall(None, replaced_function, (replaced_col,)),
    ]
    assert list(f3) == expected


def test_subscriptable() -> None:
    c1 = Column(None, "t1", "tags")
    l1 = Literal(None, "myTag")
    s = SubscriptableReference("alias", c1, l1)

    assert list(s) == [c1, l1, s]

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Literal):
            return Literal(None, "myOtherTag")
        return e

    replaced = s.transform(replace_col)
    l2 = Literal(None, "myOtherTag")
    assert list(replaced) == [c1, l2, SubscriptableReference("alias", c1, l2)]


def test_hash() -> None:
    """
    Ensures expressions are hashable
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall(None, "f1", (column1, column2))
    literal = Literal(None, "blablabla")
    function_2 = CurriedFunctionCall(None, function_1, (column1,))
    lm = Lambda(None, ("x", "y"), FunctionCall(None, "test", (Argument(None, "x"),)))

    s: Set[Expression] = set()
    s.add(column1)
    s.add(column2)
    s.add(function_1)
    s.add(literal)
    s.add(function_2)
    s.add(lm)

    assert len(s) == 6


TEST_CASES = [
    (Column(None, "t1", "c1"), "t1.c1"),
    (Column(None, None, "c1"), "c1"),
    (Literal(None, "meowmeow"), "'meowmeow'"),
    (Literal(None, 123), "123"),
    (Literal(None, False), "False"),
    (
        Literal(None, datetime(2020, 4, 20, 16, 20)),
        "datetime(2020-04-20T16:20:00)",
    ),
    (Literal(None, datetime(2020, 4, 20, 16, 20).date()), "date(2020-04-20)"),
    (Literal(None, None), "None"),
    (
        SubscriptableReference(
            "catsound",
            column=Column(None, "cats", "sounds"),
            key=Literal(None, "meow"),
        ),
        "cats.sounds['meow'] AS `catsound`",
    ),
    (
        SubscriptableReference(
            "catsound",
            column=Column("kittysounds", "cats", "sounds"),
            key=Literal(None, "meow"),
        ),
        "(cats.sounds AS `kittysounds`)['meow'] AS `catsound`",
    ),
    (Column("alias", None, "c1"), "c1 AS `alias`"),
    (
        FunctionCall(None, "f1", (Column(None, "t1", "c1"), Column(None, "t1", "c2"))),
        """f1(
  t1.c1,
  t1.c2
)""",
    ),
    (
        CurriedFunctionCall(
            None,
            FunctionCall(None, "f1", (Column(None, "t1", "c1"), Column(None, "t1", "c2"))),
            (Literal(None, "hello"), Literal(None, "kitty")),
        ),
        """f1(
  t1.c1,
  t1.c2
)(
  'hello',
  'kitty'
)""",
    ),
    (
        FunctionCall(
            None,
            "f1",
            (
                FunctionCall(None, "fnested", (Column(None, "t1", "c1"),)),
                Column(None, "t1", "c2"),
            ),
        ),
        """f1(
  fnested(
    t1.c1
  ),
  t1.c2
)""",
    ),
    (
        Lambda(
            None,
            ("a", "b", "c"),
            FunctionCall(
                None,
                "some_func",
                (Argument(None, "a"), Argument(None, "b"), Argument(None, "c")),
            ),
        ),
        """(a,b,c) ->
  some_func(
    a,
    b,
    c
  )
""",
    ),
    (ArbitrarySQL(None, "COUNT(*)"), "ArbitrarySQL('COUNT(*)')"),
    (ArbitrarySQL("alias", "SUM(col)"), "ArbitrarySQL('SUM(col)') AS `alias`"),
]


@pytest.mark.parametrize("test_expr,expected_str", TEST_CASES)
def test_format(test_expr: Expression, expected_str: str) -> None:
    assert repr(test_expr) == expected_str


@pytest.mark.parametrize("test_expr,_formatted_str", TEST_CASES)
def test_functional_eq(test_expr: Expression, _formatted_str: str) -> None:
    mangled_expr = test_expr.transform(lambda expr: replace(expr, alias=uuid.uuid4().hex))
    assert test_expr != mangled_expr
    assert mangled_expr.functional_eq(test_expr)


def test_arbitrary_sql_basic() -> None:
    """Test basic ArbitrarySQL construction and properties"""
    sql = "SELECT * FROM table"
    exp = ArbitrarySQL(None, sql)

    assert exp.sql == sql
    assert exp.alias is None

    exp_with_alias = ArbitrarySQL("my_alias", sql)
    assert exp_with_alias.alias == "my_alias"


def test_arbitrary_sql_iteration() -> None:
    """Test that ArbitrarySQL iterates over itself only"""
    sql = "COUNT(*) + 1"
    exp = ArbitrarySQL(None, sql)

    result = list(exp)
    assert result == [exp]
    assert len(result) == 1


def test_arbitrary_sql_transform() -> None:
    """Test that transform applies function to self"""
    sql = "AVG(column)"
    exp = ArbitrarySQL(None, sql)

    # Transform should only apply to self, not change SQL content
    def add_alias(e: Expression) -> Expression:
        if isinstance(e, ArbitrarySQL):
            return replace(e, alias="transformed")
        return e

    transformed = exp.transform(add_alias)
    assert isinstance(transformed, ArbitrarySQL)
    assert transformed.sql == sql
    assert transformed.alias == "transformed"


def test_arbitrary_sql_transform_identity() -> None:
    """Test that transform with identity function returns self"""
    sql = "SUM(col)"
    exp = ArbitrarySQL("alias", sql)

    transformed = exp.transform(lambda e: e)
    assert transformed is exp


def test_arbitrary_sql_functional_eq() -> None:
    """Test functional equality ignores aliases"""
    sql = "MAX(value)"
    exp1 = ArbitrarySQL(None, sql)
    exp2 = ArbitrarySQL("alias1", sql)
    exp3 = ArbitrarySQL("alias2", sql)
    exp4 = ArbitrarySQL(None, "MIN(value)")

    # Same SQL, different aliases - should be functionally equal
    assert exp1.functional_eq(exp2)
    assert exp2.functional_eq(exp3)
    assert exp1.functional_eq(exp3)

    # Different SQL - not equal
    assert not exp1.functional_eq(exp4)

    # Different type - not equal
    literal = Literal(None, sql)
    assert not exp1.functional_eq(literal)


def test_arbitrary_sql_hash() -> None:
    """Test that ArbitrarySQL expressions are hashable"""
    sql1 = "COUNT(DISTINCT user_id)"
    sql2 = "SUM(amount)"

    exp1 = ArbitrarySQL(None, sql1)
    exp2 = ArbitrarySQL("alias", sql1)
    exp3 = ArbitrarySQL(None, sql2)

    # Should be able to add to set
    expr_set = {exp1, exp2, exp3}
    assert len(expr_set) == 3


def test_arbitrary_sql_in_function_call() -> None:
    """Test ArbitrarySQL as parameter in function call"""
    arbitrary = ArbitrarySQL(None, "custom_agg(col)")
    col = Column(None, "t1", "c1")
    func = FunctionCall(None, "if", (arbitrary, col, Literal(None, 0)))

    # Should iterate correctly
    result = list(func)
    assert arbitrary in result
    assert col in result


def test_arbitrary_sql_edge_cases() -> None:
    """Test edge cases like empty string, special characters"""
    # Empty string
    exp_empty = ArbitrarySQL(None, "")
    assert exp_empty.sql == ""

    # SQL with newlines
    sql_multiline = """SELECT
    col1,
    col2
FROM table"""
    exp_multiline = ArbitrarySQL(None, sql_multiline)
    assert exp_multiline.sql == sql_multiline

    # SQL with quotes and escapes
    sql_complex = "SELECT 'can''t', \"quoted\", `backticks`"
    exp_complex = ArbitrarySQL(None, sql_complex)
    assert exp_complex.sql == sql_complex

    # SQL with alias in content vs expression alias
    sql_with_alias = "column AS my_name"
    exp = ArbitrarySQL("outer_alias", sql_with_alias)
    assert exp.sql == sql_with_alias
    assert exp.alias == "outer_alias"
