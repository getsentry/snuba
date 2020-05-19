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
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    SubscriptableReference,
)


class ExpressionTranslator(
    MappingExpressionTranslator[ClickhouseExpression, FunctionCall, Column]
):
    """
    Translates a Snuba expression into a Clickhouse expression.

    As long as the Clickhouse query AST is basically the same as the Snuba one
    we add a default rule that makes a copy of the original expression.
    """

    def __init__(
        self,
        translation_rules: TranslationRules[ClickhouseExpression, FunctionCall, Column],
    ) -> None:
        default_rules = TranslationRules(
            literals=[DefaultSimpleMapper()],
            columns=[DefaultSimpleMapper()],
            # TODO: remove the DefaultSubscriptableMapper translation rule as
            # soon as we finalize the tags translation rule.
            subscriptables=[DefaultSubscriptableMapper()],
            subscriptables_columns=[DefaultSubscriptableColumn()],
            functions=[DefaultFunctionMapper()],
            curried_functions=[DefaultCurriedFunctionMapper()],
            inner_functions=[DefaultInnerFunctionMapper()],
            arguments=[DefaultSimpleMapper()],
            lambdas=[DefaultLambdaMapper()],
        )
        super().__init__(
            translation_rules=translation_rules, default_rules=default_rules
        )


TSimpleExp = TypeVar("TSimpleExp", bound=SnubaExpression)


class DefaultSimpleMapper(
    ExpressionMapper[
        TSimpleExp, ClickhouseExpression, ClickhouseExpression, FunctionCall, Column
    ]
):
    def attempt_map(
        self,
        expression: TSimpleExp,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(deepcopy(expression))


class DefaultSubscriptableColumn(
    ExpressionMapper[Column, Column, ClickhouseExpression, FunctionCall, Column]
):
    def attempt_map(
        self,
        expression: Column,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[Column]:
        return deepcopy(expression)


class DefaultFunctionMapper(
    ExpressionMapper[
        FunctionCall, ClickhouseExpression, ClickhouseExpression, FunctionCall, Column
    ]
):
    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                parameters=tuple(
                    p.accept(children_translator) for p in expression.parameters
                ),
            )
        )


class DefaultInnerFunctionMapper(
    ExpressionMapper[
        FunctionCall, FunctionCall, ClickhouseExpression, FunctionCall, Column
    ]
):
    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[FunctionCall]:
        return replace(
            expression,
            parameters=tuple(
                p.accept(children_translator) for p in expression.parameters
            ),
        )


class DefaultCurriedFunctionMapper(
    ExpressionMapper[
        CurriedFunctionCall,
        ClickhouseExpression,
        ClickhouseExpression,
        FunctionCall,
        Column,
    ]
):
    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            CurriedFunctionCall(
                alias=expression.alias,
                internal_function=children_translator.translate_inner_function(
                    expression.internal_function
                ),
                parameters=tuple(
                    p.accept(children_translator) for p in expression.parameters
                ),
            )
        )


class DefaultSubscriptableMapper(
    ExpressionMapper[
        SubscriptableReference,
        ClickhouseExpression,
        ClickhouseExpression,
        FunctionCall,
        Column,
    ]
):
    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            SubscriptableReference(
                alias=expression.alias,
                column=children_translator.translate_subscriptable_column(
                    expression.column
                ),
                key=deepcopy(expression.key),
            )
        )


class DefaultLambdaMapper(
    ExpressionMapper[
        Lambda, ClickhouseExpression, ClickhouseExpression, FunctionCall, Column
    ]
):
    def attempt_map(
        self,
        expression: Lambda,
        children_translator: MappingExpressionTranslator[
            ClickhouseExpression, FunctionCall, Column
        ],
    ) -> Optional[ClickhouseExpression]:
        return ClickhouseExpression(
            replace(
                expression,
                transformation=expression.transformation.accept(children_translator),
            )
        )
