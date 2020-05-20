from abc import ABC, abstractmethod
from typing import TypeVar

from snuba.clickhouse.query import Expression
from snuba.datasets.plans.translator.expressions import ExpressionTranslator
from snuba.query.expressions import Column, FunctionCall, Literal

TExpIn = TypeVar("TExpIn")

# TODO Explain the idea of the two steps translation with a source spec and dest spec
class ClickhouseExpressionTranslator(ExpressionTranslator[TExpIn, Expression], ABC):
    """
    This represent an expression translator that translates a query from a parametric
    type into a Clickhouse expression. Any translator that wants to produce Clickhouse
    AST Expressions should extend this class.

    This class requires the implementations to provide some specific translation methods
    with a specific output type since the Clickhouse AST allows (will allow whether or
    not it will be different than the Snuba one) some nodes to have children that have
    a more specific type than Expression (see CurriedFunctionCall).
    Requiring a translator to provide methods that produce the specific subclasses of the
    Expression makes it easier to spot invalid translation rules.
    """

    @abstractmethod
    def produce_curried_inner_func(self, exp: TExpIn) -> FunctionCall:
        """
        Translates the expression that is needed to populate a CurriedFunctionCall
        inner function.
        """
        raise NotImplementedError

    @abstractmethod
    def produce_subscriptable_column(self, exp: TExpIn) -> Column:
        """
        Translates the expression that is used to build a SubscriptableReference column
        (for as long as that expression type will exist in the Clickhouse AST)
        """
        raise NotImplementedError

    @abstractmethod
    def produce_subscriptable_key(self, exp: TExpIn) -> Literal:
        """
        Same as produce_subscriptable_column but takes care of the key.
        """
        raise NotImplementedError
