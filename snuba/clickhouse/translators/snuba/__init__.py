from abc import ABC, abstractmethod

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.query.expressions import (
    ExpressionVisitor,
    FunctionCall,
)


class SnubaClickhouseTranslator(
    ExpressionVisitor[ClickhouseExpression], ABC,
):
    @abstractmethod
    def translate_function_enforce(self, exp: FunctionCall) -> FunctionCall:
        raise NotImplementedError
