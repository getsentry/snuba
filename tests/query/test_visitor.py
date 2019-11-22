from typing import List

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Literal,
)


class DummyVisitor(ExpressionVisitor):
    def __init__(self):
        self.__visited_nodes: List[Expression] = []

    def get_visited_nodes(self) -> List[Expression]:
        return self.__visited_nodes

    def visitLiteral(self, exp: Literal) -> None:
        self.__visited_nodes.append(exp)

    def visitColumn(self, exp: Column) -> None:
        self.__visited_nodes.append(exp)

    def visitFunctionCall(self, exp: FunctionCall) -> None:
        self.__visited_nodes.append(exp)
        for param in exp.parameters:
            param.accept(self)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> None:
        self.__visited_nodes.append(exp)
        exp.internal_function.accept(self)
        for param in exp.parameters:
            param.accept(self)


def test_visit_expression():
    col1 = Column("al", "c1", "t1")
    literal1 = Literal("al2", "test")
    f1 = FunctionCall("al3", "f1", [col1, literal1])

    col2 = Column("al4", "c2", "t1")
    literal2 = Literal("al5", "test2")
    f2 = FunctionCall("al6", "f2", [col2, literal2])

    curried = CurriedFunctionCall("al7", f2, [f1])

    visitor = DummyVisitor()
    curried.accept(visitor)

    expected = [curried, f2, col2, literal2, f1, col1, literal1]

    assert visitor.get_visited_nodes() == expected
