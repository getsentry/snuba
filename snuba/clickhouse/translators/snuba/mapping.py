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
    Represents the set of rules to be used to configure a
    SnubaClickhouseMappingTranslator. It encapsulates different sequences
    of rules.
    Each one translates a different expression type. The types of the
    mappers impose the subset of valid translations rules since each one
    of the mappers types limits what can be translated into what (like
    Columns can only become Columns, Functions or Literals).

    This is because only some pairs of expression types (Snuba,
    Clickhouse) are allowed during translation so we can guarantee any
    configuration provided by the user will either produce a valid AST
    or refuse to translate.

    See allowed.py for the valid translation rules and their reasoning.
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
    Translates a Snuba expression into an clickhouse query expression
    according to a specification provide by the caller.

    The translation of every node in the expression is performed by a
    series of rules that extend ExpressionMapper.
    Rules are applied in sequence. Given an expression, the first valid
    rule for such expression is applied and the result is returned. If
    no rule can translate such expression an exception is raised.
    A rule can delegate the translation of its children back to this
    translator.

    Each rule only has context around the expression provided and its
    children. It does not have general context around the query or around
    the expression's ancestors in the AST.
    This approach implies that, while rules are specific to the
    relationship between dataset (later entities) and storage, this class
    keeps the responsibility of orchestrating the translation process.

    It is possible to compose different, independently defined, sets of
    rules that are applied in a single pass over the AST.
    This allows us to support joins which can be translated by simply
    concatenating rule sets associated with each storage involved.

    This relies on a visitor so that we can statically enforce that no
    expression subtype is added to the code base without properly support
    it in all translators. The downside is verbosity.
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

    def visit_literal(self, exp: Literal) -> Expression:
        return apply_mappers(exp, self.__translation_rules.literals, self)

    def visit_column(self, exp: Column) -> Expression:
        return apply_mappers(exp, self.__translation_rules.columns, self)

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> Expression:
        return apply_mappers(exp, self.__translation_rules.subscriptables, self)

    def visit_function_call(self, exp: FunctionCall) -> Expression:
        return apply_mappers(exp, self.__translation_rules.functions, self)

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> Expression:
        return apply_mappers(exp, self.__translation_rules.curried_functions, self)

    def visit_argument(self, exp: Argument) -> Expression:
        return apply_mappers(exp, self.__translation_rules.arguments, self)

    def visit_lambda(self, exp: Lambda) -> Expression:
        return apply_mappers(exp, self.__translation_rules.lambdas, self)

    def translate_function_strict(self, exp: FunctionCall) -> FunctionCall:
        """
        Unfortunately it is not possible to avoid this assertion.
        Though the structure of TranslationMappers guarantees that this
        assertion can never fail since it defines the valid translations
        and it statically requires a FunctionCallMapper to translate a
        FunctionCall.
        FunctionCallMapper returns FunctionCall as return type, thus
        always satisfying the assertion.
        """
        f = exp.accept(self)
        assert isinstance(f, FunctionCall)
        return f
