from abc import ABC
from copy import deepcopy
from dataclasses import replace
from typing import Generic, Optional, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.datasets.plans.translator.visitor import (
    ExpressionMapper,
    MappingExpressionTranslator,
    TranslationRules,
)
from snuba.query.expressions import Argument, Column, CurriedFunctionCall
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class ExpressionTranslator(
    MappingExpressionTranslator[ClickhouseExpression, FunctionCall, Column, Literal]
):
    """
    Translates a Snuba expression into a Clickhouse expression.

    As long as the Clickhouse query AST is basically the same as the Snuba one
    we add a default rule that makes a copy of the original expression.
    """

    def __init__(
        self,
        translation_rules: TranslationRules[
            ClickhouseExpression, FunctionCall, Column, Literal
        ],
    ) -> None:
        default_rules = TranslationRules(
            generic_exp=[
                DefaultSimpleMapper(),
                DefaultFunctionMapper(),
                DefaultCurriedFunctionMapper(),
                DefaultSubscriptableMapper(),
                DefaultLambdaMapper(),
            ],
            inner_functions=[DefaultInnerFunctionMapper()],
            subscriptable_cols=[DefaultSubscriptableCol()],
            subscript_keys=[DefaultSubscriptKey()],
        )
        super().__init__(
            translation_rules=translation_rules, default_rules=default_rules
        )


TSimpleExp = TypeVar("TSimpleExp", bound=SnubaExpression)

TranslatorAlias = MappingExpressionTranslator[
    ClickhouseExpression, FunctionCall, Column, Literal
]

TExpIn = TypeVar("TExpIn", bound=SnubaExpression)

# THe type of the output of the individual mapper.
TMapperOut = TypeVar("TMapperOut")


class ClickhouseMapper(
    Generic[TExpIn, TMapperOut],
    ExpressionMapper[
        TExpIn, TMapperOut, ClickhouseExpression, FunctionCall, Column, Literal
    ],
    ABC,
):
    pass


class DefaultSimpleMapper(ClickhouseMapper[TSimpleExp, ClickhouseExpression]):
    def attempt_map(
        self, expression: TSimpleExp, children_translator: TranslatorAlias,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, (Argument, Column, Lambda, Literal)):
            return ClickhouseExpression(deepcopy(expression))
        else:
            return None


class DefaultFunctionMapper(ClickhouseMapper[SnubaExpression, ClickhouseExpression]):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: TranslatorAlias,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, FunctionCall):
            return ClickhouseExpression(
                replace(
                    expression,
                    parameters=tuple(
                        children_translator.translate_expression(p)
                        for p in expression.parameters
                    ),
                )
            )
        else:
            return None


class DefaultCurriedFunctionMapper(
    ClickhouseMapper[SnubaExpression, ClickhouseExpression]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: TranslatorAlias,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, CurriedFunctionCall):
            return ClickhouseExpression(
                CurriedFunctionCall(
                    alias=expression.alias,
                    internal_function=children_translator.translate_inner_function(
                        expression.internal_function
                    ),
                    parameters=tuple(
                        children_translator.translate_expression(p)
                        for p in expression.parameters
                    ),
                )
            )
        else:
            return None


class DefaultInnerFunctionMapper(ClickhouseMapper[FunctionCall, FunctionCall]):
    def attempt_map(
        self, expression: FunctionCall, children_translator: TranslatorAlias,
    ) -> Optional[FunctionCall]:
        return replace(
            expression,
            parameters=tuple(
                children_translator.translate_expression(p)
                for p in expression.parameters
            ),
        )


class DefaultSubscriptableMapper(
    ClickhouseMapper[SnubaExpression, ClickhouseExpression]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: TranslatorAlias,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, SubscriptableReference):
            return ClickhouseExpression(
                SubscriptableReference(
                    alias=expression.alias,
                    column=children_translator.translate_subscriptable_col(
                        expression.column
                    ),
                    key=children_translator.translate_subscript_key(expression.key),
                )
            )
        else:
            return None


class DefaultSubscriptableCol(ClickhouseMapper[Column, Column]):
    def attempt_map(
        self, expression: Column, children_translator: TranslatorAlias,
    ) -> Optional[Column]:
        return deepcopy(expression)


class DefaultSubscriptKey(ClickhouseMapper[Literal, Literal]):
    def attempt_map(
        self, expression: Literal, children_translator: TranslatorAlias,
    ) -> Optional[Literal]:
        return deepcopy(expression)


class DefaultLambdaMapper(ClickhouseMapper[SnubaExpression, ClickhouseExpression]):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: TranslatorAlias,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, Lambda):
            return ClickhouseExpression(
                replace(
                    expression,
                    transformation=children_translator.translate_expression(
                        expression.transformation
                    ),
                )
            )
        else:
            return None
