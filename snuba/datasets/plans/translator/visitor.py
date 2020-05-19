from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, Optional, Sequence, TypeVar

from snuba.query.expressions import Column, Expression, FunctionCall, Literal

# The type of a generic translated expression without constraint on the data type
TGenericExpOut = TypeVar("TGenericExpOut")
# THe type of the translated CurriedFunction.inner_function
TInnerFuncOut = TypeVar("TInnerFuncOut")
# The type of a translated column in a Subscriptable reference
TSubscriptableColOut = TypeVar("TSubscriptableColOut")
# The type of the translated key in a Subscriptable reference
TSubscriptKeyOut = TypeVar("TSubscriptKeyOut")


@dataclass(frozen=True)
class TranslationRules(
    Generic[TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut]
):
    """
    Represents the set of rules to be used to configure a MappingExpressionTranslator.
    It encapsulates different sequences of rules. Each is meant to be used for a
    specific expression in the AST, like CurriedFunction inner functions.
    It provides a concat method to combine multiple sets of rules.

    see MappingExpressionTranslator for more context
    """

    generic_exp: Sequence[
        ExpressionMapper[
            Expression,
            TGenericExpOut,
            TGenericExpOut,
            TInnerFuncOut,
            TSubscriptableColOut,
            TSubscriptKeyOut,
        ]
    ] = field(default_factory=list)
    inner_functions: Sequence[
        ExpressionMapper[
            FunctionCall,
            TInnerFuncOut,
            TGenericExpOut,
            TInnerFuncOut,
            TSubscriptableColOut,
            TSubscriptKeyOut,
        ]
    ] = field(default_factory=list)
    subscriptable_cols: Sequence[
        ExpressionMapper[
            Column,
            TSubscriptableColOut,
            TGenericExpOut,
            TInnerFuncOut,
            TSubscriptableColOut,
            TSubscriptKeyOut,
        ]
    ] = field(default_factory=list)
    subscript_keys: Sequence[
        ExpressionMapper[
            Literal,
            TSubscriptKeyOut,
            TGenericExpOut,
            TInnerFuncOut,
            TSubscriptableColOut,
            TSubscriptKeyOut,
        ]
    ] = field(default_factory=list)

    def concat(
        self,
        spec: TranslationRules[
            TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut
        ],
    ) -> TranslationRules[
        TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut
    ]:
        return TranslationRules(
            generic_exp=[*self.generic_exp, *spec.generic_exp],
            inner_functions=[*self.inner_functions, *spec.inner_functions],
            subscriptable_cols=[*self.subscriptable_cols, *spec.subscriptable_cols],
            subscript_keys=[*self.subscript_keys, *spec.subscript_keys],
        )


TExpIn = TypeVar("TExpIn", bound=Expression)

# THe type of the output of the individual mapper.
TMapperOut = TypeVar("TMapperOut")


class ExpressionMapper(
    ABC,
    Generic[
        TExpIn,
        TMapperOut,
        TGenericExpOut,
        TInnerFuncOut,
        TSubscriptableColOut,
        TSubscriptKeyOut,
    ],
):
    """
    One translation rule used by the MappingExpressionTranslator to translate an
    expression.

    see MappingExpressionTranslator for more context
    """

    @abstractmethod
    def attempt_map(
        self,
        expression: TExpIn,
        children_translator: MappingExpressionTranslator[
            TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut
        ],
    ) -> Optional[TMapperOut]:
        """
        Translates an expression if this rule matches such expression. If not, it
        returns None.

        It receives an instance of a MappingExpressionTranslator to take care of
        children that this rule is not capable, or not supposed to, translate.
        These node should be delegated to the children_translator.
        """
        raise NotImplementedError


class MappingExpressionTranslator(
    Generic[TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut]
):
    """
    Translates a Snuba query expression into an physical query expression (like a
    Clickhouse query expression).

    The translation of every node in the expression is performed by a series of rules
    that extend ExpressionMapper.
    Rules are applied in sequence. Given an expression, the first valid rule for such
    expression is applied and the result is returned. If no rule can translate such
    expression an exception is raised.
    A rule can delegate the translation of its children back to this translator.

    Each rule only has context around the expression provided and its children. It does
    not have general context around the query or around the expression's ancestors in
    the AST.
    This approach implies that, while rules are specific to the relationship between
    dataset (later entities) and storage, this class keeps the responsibility of
    orchestrating the translation process.

    It is possible to compose different, independently defined, sets of rules that are
    applied in a single pass over the AST.
    This allows us to support joins and multi-step translations (for multi table
    storages) as an example:
    Joins can be supported by simply concatenating rule sets associated with each storage.
    Multi-step (still TODO) translations can be supported by applying a second sequence of
    rules to the result of the first one for each node in the expression to be translated.
    """

    def __init__(
        self,
        translation_rules: TranslationRules[
            TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut
        ],
        default_rules: Optional[
            TranslationRules[
                TGenericExpOut, TInnerFuncOut, TSubscriptableColOut, TSubscriptKeyOut
            ]
        ],
    ) -> None:
        self.__translation_rules = (
            translation_rules
            if not default_rules
            else translation_rules.concat(default_rules)
        )

    def translate_expression(self, exp: Expression) -> TGenericExpOut:
        return self.__map_expression(exp, self.__translation_rules.generic_exp)

    def translate_inner_function(self, exp: FunctionCall) -> TInnerFuncOut:
        return self.__map_expression(exp, self.__translation_rules.inner_functions)

    def translate_subscriptable_col(self, exp: Column) -> TSubscriptableColOut:
        return self.__map_expression(exp, self.__translation_rules.subscriptable_cols)

    def translate_subscript_key(self, exp: Literal) -> TSubscriptKeyOut:
        return self.__map_expression(exp, self.__translation_rules.subscript_keys)

    def __map_expression(
        self,
        exp: TExpIn,
        rules: Sequence[
            ExpressionMapper[
                TExpIn,
                TMapperOut,
                TGenericExpOut,
                TInnerFuncOut,
                TSubscriptableColOut,
                TSubscriptKeyOut,
            ]
        ],
    ) -> TMapperOut:
        for r in rules:
            ret = r.attempt_map(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
