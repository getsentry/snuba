from typing import List

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    Argument,
)


class DummyVisitor(ExpressionVisitor[List[Expression]]):
    def __init__(self):
        self.__visited_nodes: List[Expression] = []

    def get_visited_nodes(self) -> List[Expression]:
        return self.__visited_nodes

    def visitLiteral(self, exp: Literal) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visitColumn(self, exp: Column) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visitFunctionCall(self, exp: FunctionCall) -> List[Expression]:
        ret = []
        self.__visited_nodes.append(exp)
        ret.append(exp)
        for param in exp.parameters:
            ret.extend(param.accept(self))
        return ret

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> List[Expression]:
        ret = []
        self.__visited_nodes.append(exp)
        ret.append(exp)
        ret.extend(exp.internal_function.accept(self))
        for param in exp.parameters:
            ret.extend(param.accept(self))
        return ret

    def visitArgument(self, exp: Argument) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visitLambda(self, exp: Lambda) -> List[Expression]:
        self.__visited_nodes.append(exp)
        self.__visited_nodes.append(exp.transform.accept(self))
        ret = [exp]
        ret.extend(exp.transform.accept(self))
        return ret


def test_visit_expression():
    col1 = Column("al", "c1", "t1")
    literal1 = Literal("al2", "test")
    f1 = FunctionCall("al3", "f1", [col1, literal1])

    col2 = Column("al4", "c2", "t1")
    literal2 = Literal("al5", "test2")
    f2 = FunctionCall("al6", "f2", [col2, literal2])

    curried = CurriedFunctionCall("al7", f2, [f1])

    visitor = DummyVisitor()
    ret = curried.accept(visitor)

    expected = [curried, f2, col2, literal2, f1, col1, literal1]

    # Tests the state changes on the Visitor
    assert visitor.get_visited_nodes() == expected
    # Tests the return value of the visitor
    assert ret == expected
