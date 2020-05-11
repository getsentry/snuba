from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, Optional, Sequence, TypeVar

from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)

TExpOut = TypeVar("TExpOut")


@dataclass(frozen=True)
class TranslationRules(Generic[TExpOut]):
    """
    Represents the set of rules to be used to configure a MappingExpressionTranslator.
    It encapsulates a sequence or rules for each AST node type.
    It provides a concat method to combine multiple sets of rules.

    see MappingExpressionTranslator for more context
    """

    literals: Sequence[ExpressionMapper[Literal, TExpOut]] = field(default_factory=list)
    columns: Sequence[ExpressionMapper[Column, TExpOut]] = field(default_factory=list)
    subscriptables: Sequence[ExpressionMapper[SubscriptableReference, TExpOut]] = field(
        default_factory=list
    )
    functions: Sequence[ExpressionMapper[FunctionCall, TExpOut]] = field(
        default_factory=list
    )
    curried_functions: Sequence[ExpressionMapper[CurriedFunctionCall, TExpOut]] = field(
        default_factory=list
    )
    arguments: Sequence[ExpressionMapper[Argument, TExpOut]] = field(
        default_factory=list
    )
    lambdas: Sequence[ExpressionMapper[Lambda, TExpOut]] = field(default_factory=list)

    def concat(self, spec: TranslationRules[TExpOut]) -> TranslationRules[TExpOut]:
        return TranslationRules(
            literals=[*self.literals, *spec.literals],
            columns=[*self.columns, *spec.columns],
            subscriptables=[*self.subscriptables, *spec.subscriptables],
            functions=[*self.functions, *spec.functions],
            curried_functions=[*self.curried_functions, *spec.curried_functions],
            arguments=[*self.arguments, *spec.arguments],
            lambdas=[*self.lambdas, *spec.lambdas],
        )


TExpIn = TypeVar("TExpIn", bound=Expression)


class ExpressionMapper(ABC, Generic[TExpIn, TExpOut]):
    """
    One translation rule used by the MappingExpressionTranslator to translate an
    expression.

    see MappingExpressionTranslator for more context
    """

    @abstractmethod
    def attemptMap(
        self, expression: TExpIn, children_translator: ExpressionVisitor[TExpOut]
    ) -> Optional[TExpOut]:
        """
        Translates an expression if this rule matches such expression. If not, it
        returns None.

        It receives an instance of a translating visitor to take care of children
        that this rule is not capable, or not supposed to, translate. These node
        should be delegated to the children_translator.
        """
        raise NotImplementedError


class MappingExpressionTranslator(ExpressionVisitor[TExpOut]):
    """
    Translates a Snuba query expression into an physical query expression (like a
    Clickhouse query expression).

    The translation of every node in the expression is performed by a series of rules
    that extend ExpressionMapper.
    Rules are applied in sequence. Given an expression, the first valid rule for such
    expression is applied and the result is returned. If no rule can translate such
    expression an exception is raised.
    A rule can delegate the translation of its children back to this visitor.

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

    The visitor approach, while a bit verbose, allows us to impose some static
    guarantees of correctness of the translator. It is impossible, for example, to add a
    new node type to the Snuba AST and ignoring it in the translator (a method
    implementation would be missing).
    """

    def __init__(
        self,
        translation_rules: TranslationRules[TExpOut],
        default_rules: Optional[TranslationRules[TExpOut]],
    ) -> None:
        self.__translation_rules = (
            translation_rules
            if not default_rules
            else translation_rules.concat(default_rules)
        )

    def visitLiteral(self, exp: Literal) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.literals)

    def visitColumn(self, exp: Column) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.columns)

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.subscriptables)

    def visitFunctionCall(self, exp: FunctionCall) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.functions)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.curried_functions)

    def visitArgument(self, exp: Argument) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.arguments)

    def visitLambda(self, exp: Lambda) -> TExpOut:
        return self.__map_expression(exp, self.__translation_rules.lambdas)

    def __map_expression(
        self, exp: TExpIn, rules: Sequence[ExpressionMapper[TExpIn, TExpOut]],
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
