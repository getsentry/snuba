from copy import deepcopy
from dataclasses import replace
from typing import Optional, TypeVar

from snuba.clickhouse.query import Expression
from snuba.datasets.plans.translator.visitor import (
    ExpressionMapper,
    MappingExpressionTranslator,
    TranslationRules,
)
from snuba.query.expressions import (
    CurriedFunctionCall,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    SubscriptableReference,
)

TSimpleExp = TypeVar("TSimpleExp", bound=Expression)


class ExpressionTranslator(MappingExpressionTranslator[Expression]):
    def __init__(self, translation_rules: TranslationRules[Expression]) -> None:
        default = TranslationRules(
            literals=[DefaultSimpleMapper()],
            columns=[DefaultSimpleMapper()],
            subscriptables=[DefaultSubscriptableFunctionMapper()],
            functions=[DefaultFunctionMapper()],
            curried_functions=[DefaultCurriedFunctionMapper()],
            arguments=[DefaultSimpleMapper()],
            lambdas=[DefaultLambdaMapper()],
        )
        super().__init__(translation_rules=translation_rules, default_rules=default)


class DefaultSimpleMapper(ExpressionMapper[TSimpleExp, Expression]):
    def attemptMap(
        self, expression: TSimpleExp, children_translator: ExpressionVisitor[Expression]
    ) -> Optional[Expression]:
        return deepcopy(expression)


class DefaultFunctionMapper(ExpressionMapper[FunctionCall, Expression]):
    def attemptMap(
        self,
        expression: FunctionCall,
        children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        return replace(
            expression,
            parameters=tuple(
                e.accept(children_translator) for e in expression.parameters
            ),
        )


class DefaultCurriedFunctionMapper(ExpressionMapper[CurriedFunctionCall, Expression]):
    def attemptMap(
        self,
        expression: CurriedFunctionCall,
        children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        return replace(
            expression,
            internal_function=expression.internal_function.accept(children_translator),
            parameters=tuple(
                e.accept(children_translator) for e in expression.parameters
            ),
        )


class DefaultSubscriptableFunctionMapper(
    ExpressionMapper[SubscriptableReference, Expression]
):
    def attemptMap(
        self,
        expression: SubscriptableReference,
        children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        return replace(
            expression,
            column=expression.column.accept(children_translator),
            key=expression.key.accept(children_translator),
        )


class DefaultLambdaMapper(ExpressionMapper[Lambda, Expression]):
    def attemptMap(
        self, expression: Lambda, children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        return replace(
            expression,
            transformation=expression.transformation.accept(children_translator),
        )
