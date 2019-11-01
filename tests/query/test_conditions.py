from snuba.query.conditions import BasicCondition, AndCondition, Operator, OrCondition
from snuba.query.expressions import FunctionCall, Column


def test_basic_condition() -> None:
    c = Column("a1", "c1", "t1")
    f1 = FunctionCall("a2", "f", [c])
    c2 = Column("a2", "c2", "t1")

    condition = BasicCondition(f1, Operator.EQ, c2)
    ret = list(condition.iterate())
    expected = [condition, f1, c, c2]

    assert ret == expected


def test_nested_simple_condition() -> None:
    c1 = Column("a1", "c1", "t1")
    c2 = Column("a2", "c2", "t1")

    co1 = BasicCondition(c1, Operator.EQ, c2)
    co2 = BasicCondition(c1, Operator.EQ, c2)
    or1 = OrCondition([co1, co2])

    co4 = BasicCondition(c1, Operator.EQ, c2)
    co5 = BasicCondition(c1, Operator.EQ, c2)
    or2 = OrCondition([co4, co5])
    and1 = AndCondition([or1, or2])

    ret = list(and1.iterate())
    expected = [and1, or1, co1, c1, c2, co2, c1, c2, or2, co4, c1, c2, co5, c1, c2]

    assert ret == expected
