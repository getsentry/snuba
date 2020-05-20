from __future__ import annotations


from copy import deepcopy
from dataclasses import dataclass, field
from dataclasses import replace
from typing import Generic, Optional, Sequence, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.datasets.plans.translator.visitor import (
    ExpressionMapper,
    ExpressionTranslator,
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
class ClickhouseTranslationRules(Generic[TExpIn]):
    """
    Represents the set of rules to be used to configure a MappingExpressionTranslator.
    It encapsulates different sequences of rules. Each is meant to be used for a
    specific expression in the AST, like CurriedFunction inner strict_functions.
    It provides a concat method to combine multiple sets of rules.

    see MappingExpressionTranslator for more context
    """

    generic_exp: Sequence[
        ExpressionMapper[
            TExpIn, ClickhouseExpression, ClickhouseExpressionTranslator[TExpIn]
        ]
    ] = field(default_factory=list)

    # TODO: Explain why we start from TExpIn
    strict_functions: Sequence[
        ExpressionMapper[TExpIn, FunctionCall, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    strict_columns: Sequence[
        ExpressionMapper[TExpIn, Column, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    strict_literals: Sequence[
        ExpressionMapper[TExpIn, Literal, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    def concat(
        self, spec: ClickhouseTranslationRules[TExpIn]
    ) -> ClickhouseTranslationRules[TExpIn]:

        return ClickhouseTranslationRules(
            generic_exp=[*self.generic_exp, *spec.generic_exp],
            strict_functions=[*self.strict_functions, *spec.strict_functions],
            strict_columns=[*self.strict_columns, *spec.strict_columns],
            strict_literals=[*self.strict_literals, *spec.strict_literals],
        )


class ClickhouseExpressionTranslator(
    ExpressionTranslator[TExpIn, ClickhouseExpression]
):
    def translate_to_function(self, exp: TExpIn) -> FunctionCall:
        raise NotImplementedError

    def translate_to_column(self, exp: TExpIn) -> Column:
        raise NotImplementedError

    def translate_to_literal(self, exp: TExpIn) -> Literal:
        raise NotImplementedError


class ClickhouseRuleBasedTranslator(ClickhouseExpressionTranslator[TExpIn]):
    def __init__(self, translation_rules: ClickhouseTranslationRules[TExpIn]) -> None:
        self.__translation_rules = translation_rules

    def translate_expression(self, exp: TExpIn) -> ClickhouseExpression:
        func = self.__map_expression(exp, self.__translation_rules.strict_functions)
        if func is not None:
            return ClickhouseExpression(func)
        col = self.__map_expression(exp, self.__translation_rules.strict_columns)
        if col is not None:
            return ClickhouseExpression(col)
        lit = self.__map_expression(exp, self.__translation_rules.strict_literals)
        if lit is not None:
            return ClickhouseExpression(lit)

        return self.__map_expression_assert(exp, self.__translation_rules.generic_exp)

    def translate_to_function(self, exp: TExpIn) -> FunctionCall:
        return self.__map_expression_assert(
            exp, self.__translation_rules.strict_functions
        )

    def translate_to_column(self, exp: TExpIn) -> Column:
        return self.__map_expression_assert(
            exp, self.__translation_rules.strict_columns
        )

    def translate_to_literal(self, exp: TExpIn) -> Literal:
        return self.__map_expression_assert(
            exp, self.__translation_rules.strict_literals
        )

    def __map_expression_assert(
        self,
        exp: TExpIn,
        rules: Sequence[
            ExpressionMapper[TExpIn, TExpOut, ClickhouseExpressionTranslator[TExpIn]]
        ],
    ) -> TExpOut:
        ret = self.__map_expression(exp, rules)
        assert ret is not None, f"Cannot map expression {exp}"
        return ret

    def __map_expression(
        self,
        exp: TExpIn,
        rules: Sequence[
            ExpressionMapper[TExpIn, TExpOut, ClickhouseExpressionTranslator[TExpIn]]
        ],
    ) -> Optional[TExpOut]:
        for r in rules:
            ret = r.attempt_map(exp, self)
            if ret is not None:
                return ret
        return None


class SnubaClickhouseExpressionTranslator(
    ClickhouseRuleBasedTranslator[SnubaExpression]
):
    def __init__(
        self, translation_rules: ClickhouseTranslationRules[SnubaExpression]
    ) -> None:
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
        print(expression)
        if isinstance(expression, (Argument, Column, Lambda, Literal)):
            return ClickhouseExpression(deepcopy(expression))
        else:
            return None


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
    ExpressionMapper[
        SnubaExpression, FunctionCall, ClickhouseExpressionTranslator[SnubaExpression]
    ]
):
    def attempt_map(
        self,
        expression: SnubaExpression,
        children_translator: ClickhouseExpressionTranslator[SnubaExpression],
    ) -> Optional[FunctionCall]:
        if isinstance(expression, FunctionCall):
            return replace(
                expression,
                parameters=tuple(
                    children_translator.translate_expression(p)
                    for p in expression.parameters
                ),
            )
        return None


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
        if isinstance(expression, Column):
            return deepcopy(expression)
        return None


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
        if isinstance(expression, Literal):
            return deepcopy(expression)
        return None


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
