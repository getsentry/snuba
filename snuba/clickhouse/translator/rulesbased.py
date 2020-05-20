from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Sequence, TypeVar

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translator import ClickhouseExpressionTranslator
from snuba.datasets.plans.translator.mapper import ExpressionMapper
from snuba.query.expressions import Column, FunctionCall, Literal

TExpIn = TypeVar("TExpIn")

# TODO explain the edfault crap
@dataclass(frozen=True)
class TranslationRules(Generic[TExpIn]):
    """
    Represents the set of rules to be used to configure a RuleBasedTranslator.
    It encapsulates different sequences of rules. Each one produces a different
    expression type, this is because, in a Clickhouse AST, several nodes have children
    of a specific type, so we need strictly typed rules that produce those specific
    types to guarantee we produce a valid AST.

    This is parametric with respect to the input expression type so we will be able to
    to use this translator to either translate a Snuba expression into a clickhouse
    expression as well as to transform a Clickhouse expression into another one, for
    query processors.
    The downside of keeping TExpIn parametric instead of hardcoding the Snuba expression
    is that all ExpressionMapper have to take in the same type even in cases where we
    could be stricter. Being able to translate both ASTs with this abstractions seem
    to be a reasonable tradeoff.
    """

    generic_exp: Sequence[
        ExpressionMapper[
            TExpIn, ClickhouseExpression, ClickhouseExpressionTranslator[TExpIn]
        ]
    ] = field(default_factory=list)

    # Mapping rules for the CurriedFunctionCall inner function
    curried_inner_producers: Sequence[
        ExpressionMapper[TExpIn, FunctionCall, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    # Mapping rules for the Columns referenced by SubscriptableReference.
    subscript_col_producers: Sequence[
        ExpressionMapper[TExpIn, Column, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    # Mapping rules for the Keys in a SubscriptableColumnReference.
    subscript_key_producers: Sequence[
        ExpressionMapper[TExpIn, Literal, ClickhouseExpressionTranslator[TExpIn]]
    ] = field(default_factory=list)

    def concat(self, spec: TranslationRules[TExpIn]) -> TranslationRules[TExpIn]:
        return TranslationRules(
            generic_exp=[*self.generic_exp, *spec.generic_exp],
            curried_inner_producers=[
                *self.curried_inner_producers,
                *spec.curried_inner_producers,
            ],
            subscript_col_producers=[
                *self.subscript_col_producers,
                *spec.subscript_col_producers,
            ],
            subscript_key_producers=[
                *self.subscript_key_producers,
                *spec.subscript_key_producers,
            ],
        )


TExpOut = TypeVar("TExpOut")


class RuleBasedTranslator(ClickhouseExpressionTranslator[TExpIn]):
    """
    Translates an expression into an clickhouse query expression.

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

    def __init__(self, translation_rules: TranslationRules[TExpIn]) -> None:
        self.__translation_rules = translation_rules

    def translate_expression(self, exp: TExpIn) -> ClickhouseExpression:
        return self.__map_expression(exp, self.__translation_rules.generic_exp)

    def produce_curried_inner_func(self, exp: TExpIn) -> FunctionCall:
        return self.__map_expression(
            exp, self.__translation_rules.curried_inner_producers
        )

    def produce_subscriptable_column(self, exp: TExpIn) -> Column:
        return self.__map_expression(
            exp, self.__translation_rules.subscript_col_producers
        )

    def produce_subscriptable_key(self, exp: TExpIn) -> Literal:
        return self.__map_expression(
            exp, self.__translation_rules.subscript_key_producers
        )

    def __map_expression(
        self,
        exp: TExpIn,
        rules: Sequence[
            ExpressionMapper[TExpIn, TExpOut, ClickhouseExpressionTranslator[TExpIn]]
        ],
    ) -> TExpOut:
        for r in rules:
            ret = r.attempt_map(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
