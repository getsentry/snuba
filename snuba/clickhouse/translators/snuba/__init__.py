from abc import ABC, abstractmethod

from snuba.clickhouse.query import Expression
from snuba.query.expressions import ExpressionVisitor
from snuba.query.expressions import FunctionCall


class SnubaClickhouseTranslator(ExpressionVisitor[Expression], ABC):
    pass


class SnubaClickhouseStrictTranslator(SnubaClickhouseTranslator):
    @abstractmethod
    def translate_function_strict(self, exp: FunctionCall) -> FunctionCall:
        raise NotImplementedError
