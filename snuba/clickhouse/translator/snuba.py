from __future__ import annotations

from typing import Union

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.datasets.plans.translator.expressions import ExpressionTranslator
from snuba.query.expressions import Argument, Column, CurriedFunctionCall
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class SnubaClickhouseTranslator(
    ExpressionTranslator[SnubaExpression, ClickhouseExpression]
):
    def translate_expression(self, expression: SnubaExpression) -> ClickhouseExpression:
        if isinstance(expression, Column):
            return self.translate_column(expression)
        if isinstance(expression, Literal):
            return self.translate_literal(expression)
        if isinstance(expression, FunctionCall):
            return self.translate_function_call(expression)
        if isinstance(expression, CurriedFunctionCall):
            return self.translate_curried_function_call(expression)
        if isinstance(expression, SubscriptableReference):
            return self.translate_subscriptable_reference(expression)
        if isinstance(expression, Lambda):
            return self.translate_lambda(expression)
        if isinstance(expression, Argument):
            return self.translate_argument(expression)
        raise ValueError("Invalid expression to translate f{expression}")

    def translate_column(self, exp: Column) -> Union[Column, Literal, FunctionCall]:
        raise NotImplementedError

    def translate_literal(self, exp: Literal) -> Union[Literal]:
        raise NotImplementedError

    def translate_function_call(self, exp: FunctionCall) -> FunctionCall:
        raise NotImplementedError

    def translate_curried_function_call(
        self, exp: CurriedFunctionCall,
    ) -> CurriedFunctionCall:
        raise NotImplementedError

    def translate_subscriptable_reference(
        self, exp: SubscriptableReference
    ) -> Union[FunctionCall, SubscriptableReference]:
        raise NotImplementedError

    def translate_lambda(self, exp: Lambda) -> Lambda:
        raise NotImplementedError

    def translate_argument(self, exp: Argument) -> Argument:
        raise NotImplementedError
