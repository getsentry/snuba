from copy import deepcopy
from dataclasses import replace
from typing import Optional, TypeVar


from snuba.clickhouse.query import Expression
from snuba.datasets.plans.translator.mapping_rules import (
    SimpleExpressionMapper,
    StructuredExpressionMapper,
)
from snuba.datasets.plans.translator.visitor import (
    ExpressionMappingSpec,
    MappingExpressionTranslator,
)
from snuba.query.expressions import (
    CurriedFunctionCall,
    FunctionCall,
    ExpressionVisitor,
    SubscriptableReference,
    Lambda,
)

TSimpleExp = TypeVar("TSimpleExp", bound=Expression)


class DefaultSimpleMapper(SimpleExpressionMapper[TSimpleExp, Expression]):
    def attemptMap(self, expression: TSimpleExp) -> Optional[Expression]:
        return deepcopy(expression)


class DefaultFunctionMapper(StructuredExpressionMapper[FunctionCall, Expression]):
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


class DefaultCurriedFunctionMapper(
    StructuredExpressionMapper[CurriedFunctionCall, Expression]
):
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
    StructuredExpressionMapper[SubscriptableReference, Expression]
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


class DefaultLambdaMapper(StructuredExpressionMapper[Lambda, Expression]):
    def attemptMap(
        self, expression: Lambda, children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        return replace(
            expression, transformation=expression.accept(children_translator),
        )


class ClickhouseExpressionVisitor(MappingExpressionTranslator[Expression]):
    def __init__(self, mapping_specs: ExpressionMappingSpec[Expression]) -> None:
        default = ExpressionMappingSpec(
            literals=[DefaultSimpleMapper()],
            columns=[DefaultSimpleMapper()],
            subscriptables=[DefaultSubscriptableFunctionMapper()],
            functions=[DefaultFunctionMapper()],
            curried_functions=[DefaultCurriedFunctionMapper()],
            arguments=[DefaultSimpleMapper()],
            lambdas=[DefaultLambdaMapper()],
        )
        super().__init__(mapping_specs=mapping_specs, default_rules=default)
