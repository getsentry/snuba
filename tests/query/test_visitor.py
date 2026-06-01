from typing import Iterable, List

from snuba.query.expressions import (
    Argument,
    Column,
    ColumnVisitor,
    CurriedFunctionCall,
    DangerousRawSQL,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    JsonPath,
    Lambda,
    Literal,
    SubscriptableReference,
)


class DummyVisitor(ExpressionVisitor[Iterable[Expression]]):
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

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp, *exp.column.accept(self), *exp.key.accept(self)]

    def visit_function_call(self, exp: FunctionCall) -> List[Expression]:
        ret: List[Expression] = []
        self.__visited_nodes.append(exp)
        ret.append(exp)
        for param in exp.parameters:
            ret.extend(param.accept(self))
        return ret

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> List[Expression]:
        ret: List[Expression] = []
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
        self.__visited_nodes.extend(exp.transformation.accept(self))
        ret: List[Expression] = [exp]
        ret.extend(exp.transformation.accept(self))
        return ret

    def visit_dangerous_raw_sql(self, exp: DangerousRawSQL) -> List[Expression]:
        self.__visited_nodes.append(exp)
        return [exp]

    def visit_json_path(self, exp: JsonPath) -> List[Expression]:
        self.__visited_nodes.append(exp)
        ret: List[Expression] = [exp]
        ret.extend(exp.base.accept(self))
        return ret


def test_visit_expression() -> None:
    col1 = Column("al", "t1", "c1")
    literal1 = Literal("al2", "test")
    mapping = Column("al2", "t1", "tags")
    key = Literal(None, "myTag")
    tag = SubscriptableReference(None, mapping, key)
    f1 = FunctionCall("al3", "f1", (col1, literal1, tag))

    col2 = Column("al4", "t1", "c2")
    literal2 = Literal("al5", "test2")
    f2 = FunctionCall("al6", "f2", (col2, literal2))

    curried = CurriedFunctionCall("al7", f2, (f1,))

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


def test_visit_arbitrary_sql() -> None:
    """Test visitor pattern with DangerousRawSQL node"""
    col = Column("al1", "t1", "c1")
    arbitrary = DangerousRawSQL("al2", "custom_function()")
    literal = Literal("al3", "test")
    func = FunctionCall("al4", "f1", (col, arbitrary, literal))

    visitor = DummyVisitor()
    ret = func.accept(visitor)

    expected = [
        func,
        col,
        arbitrary,
        literal,
    ]

    assert visitor.get_visited_nodes() == expected
    assert ret == expected


def test_column_visitor_with_arbitrary_sql() -> None:
    """Test that ColumnVisitor handles DangerousRawSQL correctly"""

    # Expression with both columns and DangerousRawSQL
    col1 = Column(None, "t1", "column1")
    arbitrary = DangerousRawSQL(None, "COUNT(*)")  # No extractable columns
    col2 = Column(None, "t1", "column2")
    func = FunctionCall(None, "func", (col1, arbitrary, col2))

    visitor = ColumnVisitor()
    columns = func.accept(visitor)

    # Should extract only the actual columns, not from DangerousRawSQL
    assert columns == {"column1", "column2"}
