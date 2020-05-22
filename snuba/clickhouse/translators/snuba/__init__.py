from abc import ABC

from snuba.clickhouse.query import Expression
from snuba.query.expressions import ExpressionVisitor


class SnubaClickhouseTranslator(ExpressionVisitor[Expression], ABC):
    pass
