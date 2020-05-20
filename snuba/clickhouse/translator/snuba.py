from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Optional, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translator import ClickhouseExpressionTranslator
from snuba.clickhouse.translator.rulesbased import RuleBasedTranslator, TranslationRules
from snuba.datasets.plans.translator.mapper import ExpressionMapper
from snuba.query.expressions import Argument, Column, CurriedFunctionCall
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class SnubaClickhouseExpressionTranslator(RuleBasedTranslator[SnubaExpression]):
    """
    Translates a Snuba AST expression into a Clickhouse AST expression using a rule
    based translator.
    """

    def __init__(self, translation_rules: TranslationRules[SnubaExpression]) -> None:
        default_rules = TranslationRules(
            generic_exp=[
                DefaultSimpleMapper(),
                DefaultFunctionMapper(),
                DefaultCurriedFunctionMapper(),
                DefaultSubscriptableMapper(),
                DefaultLambdaMapper(),
            ],
            curried_inner_producers=[DefaultStrictFunctionMapper()],
            subscript_col_producers=[DefaultStrictColumn()],
            subscript_key_producers=[DefaultStrictLiteral()],
        )
        super().__init__(translation_rules.concat(default_rules))


TSimpleExp = TypeVar("TSimpleExp", bound=SnubaExpression)


class DefaultSimpleMapper(
    ExpressionMapper[
        TSimpleExp,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    def attempt_map(
        self,
        expression: TSimpleExp,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        return (
            ClickhouseExpression(deepcopy(expression))
            if isinstance(expression, (Argument, Column, Lambda, Literal))
            else None
        )


class DefaultFunctionMapper(
    ExpressionMapper[
        SnubaExpression,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        if not isinstance(expression, FunctionCall):
            return None

        return ClickhouseExpression(
            replace(
                expression,
                parameters=tuple(
                    children_translator.translate_expression(p)
                    for p in expression.parameters
                ),
            )
        )


class DefaultCurriedFunctionMapper(
    ExpressionMapper[
        SnubaExpression,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        if not isinstance(expression, CurriedFunctionCall):
            return None

        return ClickhouseExpression(
            CurriedFunctionCall(
                alias=expression.alias,
                internal_function=children_translator.produce_curried_inner_func(
                    expression.internal_function
                ),
                parameters=tuple(
                    children_translator.translate_expression(p)
                    for p in expression.parameters
                ),
            )
        )


class DefaultStrictFunctionMapper(
    ExpressionMapper[
        SnubaExpression, FunctionCall, ClickhouseExpressionTranslator[SnubaExpression]
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[FunctionCall]:
        if not isinstance(expression, FunctionCall):
            return None

        return replace(
            expression,
            parameters=tuple(
                children_translator.translate_expression(p)
                for p in expression.parameters
            ),
        )


class DefaultSubscriptableMapper(
    ExpressionMapper[
        SnubaExpression,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        if not isinstance(expression, SubscriptableReference):
            return None

        return ClickhouseExpression(
            SubscriptableReference(
                alias=expression.alias,
                column=children_translator.produce_subscriptable_column(
                    expression.column
                ),
                key=children_translator.produce_subscriptable_key(expression.key),
            )
        )


class DefaultStrictColumn(
    ExpressionMapper[
        SnubaExpression, Column, ClickhouseExpressionTranslator[SnubaExpression]
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[Column]:
        return deepcopy(expression) if isinstance(expression, Column) else None


class DefaultStrictLiteral(
    ExpressionMapper[
        SnubaExpression, Literal, ClickhouseExpressionTranslator[SnubaExpression]
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[Literal]:
        return deepcopy(expression) if isinstance(expression, Literal) else None


class DefaultLambdaMapper(
    ExpressionMapper[
        SnubaExpression,
        ClickhouseExpression,
        ClickhouseExpressionTranslator[SnubaExpression],
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[ClickhouseExpression]:
        if not isinstance(expression, Lambda):
            return None

        return ClickhouseExpression(
            replace(
                expression,
                transformation=children_translator.translate_expression(
                    expression.transformation
                ),
            )
        )
