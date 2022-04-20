from dataclasses import replace

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
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
from snuba.request.request_settings import RequestSettings


class _TupleUnaliasVisitor(ExpressionVisitor[Expression]):
    def __init__(self) -> None:
        # TODO: make level handling a context manager maybe
        self.__level = 0

    def visit_literal(self, exp: Literal) -> Expression:
        return exp

    def visit_column(self, exp: Column) -> Expression:
        return exp

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> Expression:
        return exp

    def visit_function_call(self, exp: FunctionCall) -> Expression:
        self.__level += 1
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        self.__level -= 1
        if self.__level != 0 and exp.function_name == "tuple" and exp.alias is not None:
            return replace(exp, alias=None, parameters=transfomed_params)
        return replace(exp, parameters=transfomed_params)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> Expression:
        self.__level += 1
        transfomed_params = tuple([param.accept(self) for param in exp.parameters])
        res = replace(
            exp,
            internal_function=exp.internal_function.accept(self),
            parameters=transfomed_params,
        )
        self.__level -= 1
        return res

    def visit_lambda(self, exp: Lambda) -> Expression:
        self.__level += 1
        res = replace(exp, transformation=exp.transformation.accept(self))
        self.__level -= 1
        return res

    def visit_argument(self, exp: Argument) -> Expression:
        return exp


class TupleUnaliaser(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        visitor = _TupleUnaliasVisitor()
        query.transform(visitor)
