from copy import deepcopy
from dataclasses import replace
from typing import Optional, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.datasets.plans.translator.visitor import (
    ExpressionMapper,
    MappingExpressionTranslator,
    TranslationRules,
)
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    CurriedFunctionCall,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    SubscriptableReference,
)


class ExpressionTranslator(MappingExpressionTranslator[ClickhouseExpression]):
    """
    Translates a Snuba expression into a Clickhouse expression.

    As long as the Clickhouse query AST is basically the same as the Snuba one
    we add a default rule that makes a copy of the original expression.
    """

    def __init__(
        self, translation_rules: TranslationRules[ClickhouseExpression]
    ) -> None:
        default_rules = TranslationRules(
            literals=[DefaultSimpleMapper()],
            columns=[DefaultSimpleMapper()],
            # TODO: remove the DefaultSubscriptableMapper translation rule as
            # soon as we finalize the tags translation rule.
            subscriptables=[DefaultSubscriptableMapper()],
            functions=[DefaultFunctionMapper()],
            curried_functions=[DefaultCurriedFunctionMapper()],
            arguments=[DefaultSimpleMapper()],
            lambdas=[DefaultLambdaMapper()],
        )
        super().__init__(
            translation_rules=translation_rules, default_rules=default_rules
        )


TSimpleExp = TypeVar("TSimpleExp", bound=SnubaExpression)


class DefaultSimpleMapper(ExpressionMapper[TSimpleExp, ClickhouseExpression]):
    def attemptMap(
        self,
        expression: TSimpleExp,
        children_translator: ExpressionVisitor[ClickhouseExpression],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(deepcopy(expression))


class DefaultFunctionMapper(ExpressionMapper[FunctionCall, ClickhouseExpression]):
    def attemptMap(
        self,
        expression: FunctionCall,
        children_translator: ExpressionVisitor[ClickhouseExpression],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                parameters=tuple(
                    p.accept(children_translator) for p in expression.parameters
                ),
            )
        )


class DefaultCurriedFunctionMapper(
    ExpressionMapper[CurriedFunctionCall, ClickhouseExpression]
):
    def attemptMap(
        self,
        expression: CurriedFunctionCall,
        children_translator: ExpressionVisitor[ClickhouseExpression],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                internal_function=expression.internal_function.accept(
                    children_translator
                ),
                parameters=tuple(
                    p.accept(children_translator) for p in expression.parameters
                ),
            )
        )


class DefaultSubscriptableMapper(
    ExpressionMapper[SubscriptableReference, ClickhouseExpression]
):
    def attemptMap(
        self,
        expression: SubscriptableReference,
        children_translator: ExpressionVisitor[ClickhouseExpression],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                column=expression.column.accept(children_translator),
                key=expression.key.accept(children_translator),
            )
        )


class DefaultLambdaMapper(ExpressionMapper[Lambda, ClickhouseExpression]):
    def attemptMap(
        self,
        expression: Lambda,
        children_translator: ExpressionVisitor[ClickhouseExpression],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                transformation=expression.transformation.accept(children_translator),
            )
        )
