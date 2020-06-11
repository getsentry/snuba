from typing import List

from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class DummyVisitor(ExpressionVisitor[List[Expression]]):
    def __init__(self) -> None:
        self.__visited_nodes: List[Expression] = []

    def get_visited_nodes(self) -> List[Expression]:
        return self.__visited_nodes

    def visit_literal(self, exp: Literal) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visit_column(self, exp: Column) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visit_subscriptable_reference(
        self, exp: SubscriptableReference
    ) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp, *exp.column.accept(self), *exp.key.accept(self)]

    def visit_function_call(self, exp: FunctionCall) -> List[Expression]:
        ret = []
        self.__visited_nodes.append(exp)
        ret.append(exp)
        for param in exp.parameters:
            ret.extend(param.accept(self))
        return ret

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> List[Expression]:
        ret = []
        self.__visited_nodes.append(exp)
        ret.append(exp)
        ret.extend(exp.internal_function.accept(self))
        for param in exp.parameters:
            ret.extend(param.accept(self))
        return ret

    def visit_argument(self, exp: Argument) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visit_lambda(self, exp: Lambda) -> List[Expression]:
        self.__visited_nodes.append(exp)
        self.__visited_nodes.append(exp.transform.accept(self))
        ret = [exp]
        ret.extend(exp.transform.accept(self))
        return ret


def test_visit_expression():
    col1 = Column("al", "t1", "c1")
    literal1 = Literal("al2", "test")
    mapping = Column("al2", "t1", "tags")
    key = Literal(None, "myTag")
    tag = SubscriptableReference(None, mapping, key)
    f1 = FunctionCall("al3", "f1", [col1, literal1, tag])

    col2 = Column("al4", "t1", "c2")
    literal2 = Literal("al5", "test2")
    f2 = FunctionCall("al6", "f2", [col2, literal2])

    curried = CurriedFunctionCall("al7", f2, [f1])

    visitor = DummyVisitor()
    ret = curried.accept(visitor)

    expected = [
        curried,
        f2,
        col2,
        literal2,
        f1,
        col1,
        literal1,
        tag,
        mapping,
        key,
    ]
    # Tests the state changes on the Visitor
    assert visitor.get_visited_nodes() == expected
    # Tests the return value of the visitor
    assert ret == expected
