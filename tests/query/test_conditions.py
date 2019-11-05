from snuba.query.conditions import BasicCondition, AndCondition, Operator, OrCondition
from snuba.query.expressions import FunctionCall, Column


def test_basic_condition() -> None:
    c = Column("a1", "c1", "t1")
    f1 = FunctionCall("a2", "f", [c])
    c2 = Column("a2", "c2", "t1")

    condition = BasicCondition(f1, Operator.EQ, c2)
    ret = list(condition.get_expressions())
    expected = [f1, c, c2]

    assert ret == expected


def test_nested_simple_condition() -> None:
    c1 = Column("a1", "c1", "t1")
    c2 = Column("a2", "c2", "t1")
    co1 = BasicCondition(c1, Operator.EQ, c2)

    c3 = Column("a1", "c1", "t1")
    c4 = Column("a2", "c2", "t1")
    co2 = BasicCondition(c3, Operator.EQ, c4)
    or1 = OrCondition([co1, co2])

    c5 = Column("a1", "c1", "t1")
    c6 = Column("a2", "c2", "t1")
    co4 = BasicCondition(c5, Operator.EQ, c6)

    c7 = Column("a1", "c1", "t1")
    c8 = Column("a2", "c2", "t1")
    co5 = BasicCondition(c7, Operator.EQ, c8)
    or2 = OrCondition([co4, co5])
    and1 = AndCondition([or1, or2])

    ret = list(and1.get_expressions())
    expected = [c1, c2, c3, c4, c5, c6, c7, c8]

    assert ret == expected
