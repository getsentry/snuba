from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from snuba.clickhouse.query import Expression
from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ArgumentMapper,
    ColumnMapper,
    CurriedFunctionCallMapper,
    FunctionCallMapper,
    LambdaMapper,
    LiteralMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.defaults import (
    DefaultArgumentMapper,
    DefaultColumnMapper,
    DefaultCurriedFunctionMapper,
    DefaultFunctionMapper,
    DefaultLambdaMapper,
    DefaultLiteralMapper,
    DefaultSubscriptableMapper,
)
from snuba.datasets.plans.translator.mapper import apply_mappers
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


@dataclass(frozen=True)
class TranslationMappers:
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

    literals: Sequence[LiteralMapper] = field(default_factory=list)
    columns: Sequence[ColumnMapper] = field(default_factory=list)
    subscriptables: Sequence[SubscriptableReferenceMapper] = field(default_factory=list)
    functions: Sequence[FunctionCallMapper] = field(default_factory=list)
    curried_functions: Sequence[CurriedFunctionCallMapper] = field(default_factory=list)
    arguments: Sequence[ArgumentMapper] = field(default_factory=list)
    lambdas: Sequence[LambdaMapper] = field(default_factory=list)

    def concat(self, spec: TranslationMappers) -> TranslationMappers:
        return TranslationMappers(
            literals=[*self.literals, *spec.literals],
            columns=[*self.columns, *spec.columns],
            subscriptables=[*self.subscriptables, *spec.subscriptables],
            functions=[*self.functions, *spec.functions],
            curried_functions=[*self.curried_functions, *spec.curried_functions],
            arguments=[*self.arguments, *spec.arguments],
            lambdas=[*self.lambdas, *spec.lambdas],
        )


class SnubaClickhouseMappingTranslator(SnubaClickhouseStrictTranslator):
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

    def __init__(self, translation_rules: TranslationMappers) -> None:
        default_rules = TranslationMappers(
            literals=[DefaultLiteralMapper()],
            columns=[DefaultColumnMapper()],
            subscriptables=[DefaultSubscriptableMapper()],
            functions=[DefaultFunctionMapper()],
            curried_functions=[DefaultCurriedFunctionMapper()],
            arguments=[DefaultArgumentMapper()],
            lambdas=[DefaultLambdaMapper()],
        )
        self.__translation_rules = translation_rules.concat(default_rules)

    def visitLiteral(self, exp: Literal) -> Expression:
        return apply_mappers(exp, self.__translation_rules.literals, self)

    def visitColumn(self, exp: Column) -> Expression:
        return apply_mappers(exp, self.__translation_rules.columns, self)

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> Expression:
        return apply_mappers(exp, self.__translation_rules.subscriptables, self)

    def visitFunctionCall(self, exp: FunctionCall) -> Expression:
        return apply_mappers(exp, self.__translation_rules.functions, self)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> Expression:
        return apply_mappers(exp, self.__translation_rules.curried_functions, self)

    def visitArgument(self, exp: Argument) -> Expression:
        return apply_mappers(exp, self.__translation_rules.arguments, self)

    def visitLambda(self, exp: Lambda) -> Expression:
        return apply_mappers(exp, self.__translation_rules.lambdas, self)

    def translate_function_strict(self, exp: FunctionCall) -> FunctionCall:
        f = exp.accept(self)
        assert isinstance(f, FunctionCall)
        return f
