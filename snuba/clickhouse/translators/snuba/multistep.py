from typing import Sequence

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translators.snuba import SnubaClickhouseTranslator
from snuba.query.expressions import Argument, Column, CurriedFunctionCall
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class MultiStepSnubaClickhouseTranslator(SnubaClickhouseTranslator):
    """
    A translator that turns a Snuba expression into a Clickhouse expression in multiple
    independent step.
    The pipeline includes:
    - a sequence of Snuba to Snuba translations.
    - one snuba to clickhouse translation step.
    - a sequence of Clickhouse to Clickhouse translations.

    This can be used for multi table storages where a SnubaClickhouseTranslator (configured
    with translation rules) does the first pass and a second translation step translates
    the result of the first step into the aggregated table schema.
    """

    def __init__(
        self,
        snuba_steps: Sequence[ExpressionVisitor[SnubaExpression]],
        snuba_clickhouse_step: SnubaClickhouseTranslator,
        clickhouse_steps: Sequence[ExpressionVisitor[ClickhouseExpression]],
    ) -> None:
        self.__snuba_steps = snuba_steps
        self.__snuba_clickhouse_step = snuba_clickhouse_step
        self.__clickhouse_steps = clickhouse_steps

    def visitLiteral(self, exp: Literal) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitColumn(self, exp: Column) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitSubscriptableReference(
        self, exp: SubscriptableReference
    ) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitFunctionCall(self, exp: FunctionCall) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitCurriedFunctionCall(
        self, exp: CurriedFunctionCall
    ) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitArgument(self, exp: Argument) -> ClickhouseExpression:
        return self.__translate(exp)

    def visitLambda(self, exp: Lambda) -> ClickhouseExpression:
        return self.__translate(exp)

    def __translate(self, expr: SnubaExpression) -> ClickhouseExpression:
        snuba_expression = expr
        for step in self.__snuba_steps:
            snuba_expression = snuba_expression.accept(step)
        clickhouse_expression = snuba_expression.accept(self.__snuba_clickhouse_step)
        for step in self.__clickhouse_steps:
            clickhouse_expression = clickhouse_expression.accept(step)
        return clickhouse_expression
