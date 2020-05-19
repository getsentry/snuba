from __future__ import annotations


from copy import deepcopy
from dataclasses import dataclass, field
from dataclasses import replace
from typing import Optional, Sequence, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.datasets.plans.translator.visitor import (
    ExpressionMapper,
    ExpressionTranslator as SnubaExpressionTranslator,
)
from snuba.query.expressions import Argument, Column, CurriedFunctionCall
from snuba.query.expressions import Expression as SnubaExpression
from snuba.query.expressions import (
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)

TExpIn = TypeVar("TExpIn")
TExpOut = TypeVar("TExpOut")


@dataclass(frozen=True)
class ClickhouseTranslationRules:
    """
    Represents the set of rules to be used to configure a MappingExpressionTranslator.
    It encapsulates different sequences of rules. Each is meant to be used for a
    specific expression in the AST, like CurriedFunction inner strict_functions.
    It provides a concat method to combine multiple sets of rules.

    see MappingExpressionTranslator for more context
    """

    generic_exp: Sequence[
        ExpressionMapper[SnubaExpression, ClickhouseExpression, ExpressionTranslator]
    ] = field(default_factory=list)

    strict_functions: Sequence[
        ExpressionMapper[FunctionCall, FunctionCall, ExpressionTranslator]
    ] = field(default_factory=list)

    strict_columns: Sequence[
        ExpressionMapper[Column, Column, ExpressionTranslator]
    ] = field(default_factory=list)

    strict_literals: Sequence[
        ExpressionMapper[Literal, Literal, ExpressionTranslator]
    ] = field(default_factory=list)

    def concat(self, spec: ClickhouseTranslationRules) -> ClickhouseTranslationRules:

        return ClickhouseTranslationRules(
            generic_exp=[*self.generic_exp, *spec.generic_exp],
            strict_functions=[*self.strict_functions, *spec.strict_functions],
            strict_columns=[*self.strict_columns, *spec.strict_columns],
            strict_literals=[*self.strict_literals, *spec.strict_literals],
        )


class ExpressionTranslator(
    SnubaExpressionTranslator[SnubaExpression, ClickhouseExpression]
):
    def __init__(self, translation_rules: ClickhouseTranslationRules) -> None:
        default_rules = ClickhouseTranslationRules(
            generic_exp=[
                DefaultSimpleMapper(),
                DefaultFunctionMapper(),
                DefaultCurriedFunctionMapper(),
                DefaultSubscriptableMapper(),
                DefaultLambdaMapper(),
            ],
            strict_functions=[DefaultStrictFunctionMapper()],
            strict_columns=[DefaultStrictColumn()],
            strict_literals=[DefaultStrictLiteral()],
        )
        self.__translation_rules = translation_rules.concat(default_rules)

    def translate_expression(self, exp: SnubaExpression) -> ClickhouseExpression:
        if isinstance(exp, FunctionCall):
            func = self.translate_to_function(exp)
            if func is not None:
                return ClickhouseExpression(func)
        if isinstance(exp, Column):
            col = self.translate_to_column(exp)
            if col is not None:
                return ClickhouseExpression(col)
        if isinstance(exp, Literal):
            lit = self.translate_to_literal(exp)
            if lit is not None:
                return ClickhouseExpression(lit)

        return self.__map_expression(exp, self.__translation_rules.generic_exp)

    def translate_to_function(self, exp: FunctionCall) -> FunctionCall:
        return self.__map_expression(exp, self.__translation_rules.strict_functions)

    def translate_to_column(self, exp: Column) -> Column:
        return self.__map_expression(exp, self.__translation_rules.strict_columns)

    def translate_to_literal(self, exp: Literal) -> Literal:
        return self.__map_expression(exp, self.__translation_rules.strict_literals)

    def __map_expression(
        self,
        exp: TExpIn,
        rules: Sequence[ExpressionMapper[TExpIn, TExpOut, ExpressionTranslator]],
    ) -> TExpOut:
        for r in rules:
            ret = r.attempt_map(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")


TSimpleExp = TypeVar("TSimpleExp", bound=SnubaExpression)


class DefaultSimpleMapper(
    ExpressionMapper[TSimpleExp, ClickhouseExpression, ExpressionTranslator]
):
    def attempt_map(
        self, expression: TSimpleExp, children_translator: ExpressionTranslator,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, (Argument, Column, Lambda, Literal)):
            return ClickhouseExpression(deepcopy(expression))
        else:
            return None


class DefaultFunctionMapper(
    ExpressionMapper[SnubaExpression, ClickhouseExpression, ExpressionTranslator]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: ExpressionTranslator,
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
    ExpressionMapper[SnubaExpression, ClickhouseExpression, ExpressionTranslator]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: ExpressionTranslator,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, CurriedFunctionCall):
            return ClickhouseExpression(
                CurriedFunctionCall(
                    alias=expression.alias,
                    internal_function=children_translator.translate_to_function(
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


class DefaultStrictFunctionMapper(
    ExpressionMapper[FunctionCall, FunctionCall, ExpressionTranslator]
):
    def attempt_map(
        self, expression: FunctionCall, children_translator: ExpressionTranslator,
    ) -> Optional[FunctionCall]:
        return replace(
            expression,
            parameters=tuple(
                children_translator.translate_expression(p)
                for p in expression.parameters
            ),
        )


class DefaultSubscriptableMapper(
    ExpressionMapper[SnubaExpression, ClickhouseExpression, ExpressionTranslator]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: ExpressionTranslator,
    ) -> Optional[ClickhouseExpression]:
        if isinstance(expression, SubscriptableReference):
            return ClickhouseExpression(
                SubscriptableReference(
                    alias=expression.alias,
                    column=children_translator.translate_to_column(expression.column),
                    key=children_translator.translate_to_literal(expression.key),
                )
            )
        else:
            return None


class DefaultStrictColumn(ExpressionMapper[Column, Column, ExpressionTranslator]):
    def attempt_map(
        self, expression: Column, children_translator: ExpressionTranslator,
    ) -> Optional[Column]:
        return deepcopy(expression)


class DefaultStrictLiteral(ExpressionMapper[Literal, Literal, ExpressionTranslator]):
    def attempt_map(
        self, expression: Literal, children_translator: ExpressionTranslator,
    ) -> Optional[Literal]:
        return deepcopy(expression)


class DefaultLambdaMapper(
    ExpressionMapper[SnubaExpression, ClickhouseExpression, ExpressionTranslator]
):
    def attempt_map(
        self, expression: SnubaExpression, children_translator: ExpressionTranslator,
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
