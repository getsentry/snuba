from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
    is_any_binary_condition,
    is_condition,
    is_in_condition,
    is_in_condition_pattern,
    is_not_in_condition,
    is_not_in_condition_pattern,
    is_unary_condition,
    condition_pattern,
    unary_condition,
)
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import Column as ColumnPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import String


def test_expressions_from_basic_condition() -> None:
    """
    Iterates over the expressions in a basic condition
    f(t1.c1) = t1.c2
    """

    c = Column(None, "t1", "c1")
    f1 = FunctionCall(None, "f", (c,))
    c2 = Column(None, "t1", "c2")

    condition = binary_condition(ConditionFunctions.EQ, f1, c2)
    ret = list(condition)
    expected = [c, f1, c2, condition]

    assert ret == expected


def test_aliased_expressions_from_basic_condition() -> None:
    """
    Iterates over the expressions in a basic condition when those expressions
    are aliased

    f(t1.c1) as a = t1.c2 as a2
    """

    c = Column(None, "t1", "c1")
    f1 = FunctionCall("a", "f", (c,))
    c2 = Column("a2", "t1", "c2")

    condition = binary_condition(ConditionFunctions.EQ, f1, c2)
    ret = list(condition)
    expected = [c, f1, c2, condition]

    assert ret == expected


def test_map_expressions_in_basic_condition() -> None:
    """
    Change the column name over the expressions in a basic condition
    """
    c = Column(None, "t1", "c1")
    f1 = FunctionCall(None, "f", (c,))
    c2 = Column(None, "t1", "c2")

    c3 = Column(None, "t1", "c3")

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c1":
            return c3
        return e

    condition = binary_condition(ConditionFunctions.EQ, f1, c2)
    condition = condition.transform(replace_col)

    condition_b = binary_condition(
        ConditionFunctions.EQ, FunctionCall(None, "f", (c3,)), c2,
    )
    ret = list(condition)
    expected = [c3, FunctionCall(None, "f", (c3,)), c2, condition_b]

    assert ret == expected


def test_nested_simple_condition() -> None:
    """
    Iterates and maps expressions over a complex Condition:
    (A=B OR A=B) AND (A=B OR A=B)
    """

    c1 = Column(None, "t1", "c1")
    c2 = Column(None, "t1", "c2")
    co1 = binary_condition(ConditionFunctions.EQ, c1, c2)

    c3 = Column(None, "t1", "c1")
    c4 = Column(None, "t1", "c2")
    co2 = binary_condition(ConditionFunctions.EQ, c3, c4)
    or1 = binary_condition(BooleanFunctions.OR, co1, co2)

    c5 = Column(None, "t1", "c1")
    c6 = Column(None, "t1", "c2")
    co4 = binary_condition(ConditionFunctions.EQ, c5, c6)

    c7 = Column(None, "t1", "c1")
    c8 = Column(None, "t1", "c2")
    co5 = binary_condition(ConditionFunctions.EQ, c7, c8)
    or2 = binary_condition(BooleanFunctions.OR, co4, co5)
    and1 = binary_condition(BooleanFunctions.AND, or1, or2)

    ret = list(and1)
    expected = [c1, c2, co1, c3, c4, co2, or1, c5, c6, co4, c7, c8, co5, or2, and1]
    assert ret == expected

    cX = Column(None, "t1", "cX")
    co1_b = binary_condition(ConditionFunctions.EQ, c1, cX)
    co2_b = binary_condition(ConditionFunctions.EQ, c3, cX)
    or1_b = binary_condition(BooleanFunctions.OR, co1_b, co2_b)
    co4_b = binary_condition(ConditionFunctions.EQ, c5, cX)
    co5_b = binary_condition(ConditionFunctions.EQ, c7, cX)
    or2_b = binary_condition(BooleanFunctions.OR, co4_b, co5_b)
    and1_b = binary_condition(BooleanFunctions.AND, or1_b, or2_b)

    def replace_col(e: Expression) -> Expression:
        if isinstance(e, Column) and e.column_name == "c2":
            return cX
        return e

    and1 = and1.transform(replace_col)
    ret = list(and1)
    expected = [
        c1,
        cX,
        co1_b,
        c3,
        cX,
        co2_b,
        or1_b,
        c5,
        cX,
        co4_b,
        c7,
        cX,
        co5_b,
        or2_b,
        and1_b,
    ]
    assert ret == expected


def test_in_condition() -> None:
    in_condition = binary_condition(
        ConditionFunctions.IN,
        Column(None, None, "tags_key"),
        literals_tuple(None, [Literal(None, "t1"), Literal(None, "t2")]),
    )
    assert is_in_condition(in_condition)

    match = is_in_condition_pattern(ColumnPattern(None, String("tags_key"))).match(
        in_condition
    )
    assert match is not None
    assert match.expression("tuple") == literals_tuple(
        None, [Literal(None, "t1"), Literal(None, "t2")]
    )
    assert match.expression("lhs") == Column(None, None, "tags_key")


def test_not_in_condition() -> None:
    not_in_condition = binary_condition(
        ConditionFunctions.NOT_IN,
        Column(None, None, "tags_key"),
        literals_tuple(None, [Literal(None, "t1"), Literal(None, "t2")]),
    )
    assert is_not_in_condition(not_in_condition)

    match = is_not_in_condition_pattern(ColumnPattern(None, String("tags_key"))).match(
        not_in_condition
    )
    assert match is not None
    assert match.expression("tuple") == literals_tuple(
        None, [Literal(None, "t1"), Literal(None, "t2")]
    )
    assert match.expression("lhs") == Column(None, None, "tags_key")


def test_is_x_condition_functions() -> None:
    eq_condition = binary_condition(
        ConditionFunctions.EQ, Column(None, None, "test"), Literal(None, "1")
    )
    assert is_any_binary_condition(eq_condition, ConditionFunctions.EQ)
    assert not is_any_binary_condition(eq_condition, ConditionFunctions.NEQ)

    un_condition = unary_condition(
        ConditionFunctions.IS_NOT_NULL, Column(None, None, "test")
    )
    assert is_unary_condition(un_condition, ConditionFunctions.IS_NOT_NULL)
    assert not is_unary_condition(un_condition, ConditionFunctions.IS_NULL)
    assert not is_unary_condition(eq_condition, ConditionFunctions.IS_NOT_NULL)

    almost_condition = FunctionCall(None, "isNotNullish", (Column(None, None, "test"),))
    assert is_condition(eq_condition)
    assert is_condition(un_condition)
    assert not is_condition(almost_condition)


def test_first_level_conditions() -> None:
    c1 = binary_condition(
        ConditionFunctions.EQ, Column(None, "table1", "column1"), Literal(None, "test"),
    )
    c2 = binary_condition(
        ConditionFunctions.EQ, Column(None, "table2", "column2"), Literal(None, "test"),
    )
    c3 = binary_condition(
        ConditionFunctions.EQ, Column(None, "table3", "column3"), Literal(None, "test"),
    )

    cond = binary_condition(
        BooleanFunctions.AND, binary_condition(BooleanFunctions.AND, c1, c2), c3,
    )
    assert get_first_level_and_conditions(cond) == [c1, c2, c3]

    cond = binary_condition(
        BooleanFunctions.AND,
        FunctionCall(
            None, "equals", (FunctionCall(None, "and", (c1, c2)), Literal(None, 1))
        ),
        c3,
    )
    assert get_first_level_and_conditions(cond) == [c1, c2, c3]

    cond = binary_condition(
        BooleanFunctions.OR, binary_condition(BooleanFunctions.AND, c1, c2), c3,
    )
    assert get_first_level_or_conditions(cond) == [
        binary_condition(BooleanFunctions.AND, c1, c2),
        c3,
    ]

    cond = binary_condition(
        ConditionFunctions.EQ,
        binary_condition(
            BooleanFunctions.OR, c1, binary_condition(BooleanFunctions.AND, c2, c3)
        ),
        Literal(None, 1),
    )
    assert get_first_level_or_conditions(cond) == [
        c1,
        binary_condition(BooleanFunctions.AND, c2, c3),
    ]


def test_binary_match() -> None:
    c1 = binary_condition(
        ConditionFunctions.EQ, Column(None, "table1", "column1"), Literal(None, "test"),
    )

    lhs = ColumnPattern(String("table1"), String("column1"))
    rhs = LiteralPattern(String("test"))

    assert (
        condition_pattern({ConditionFunctions.EQ}, lhs, rhs, True).match(c1) is not None
    )
    assert (
        condition_pattern({ConditionFunctions.EQ}, lhs, rhs, False).match(c1)
        is not None
    )
    assert (
        condition_pattern({ConditionFunctions.EQ}, rhs, lhs, True).match(c1) is not None
    )
    assert condition_pattern({ConditionFunctions.EQ}, rhs, lhs, False).match(c1) is None
