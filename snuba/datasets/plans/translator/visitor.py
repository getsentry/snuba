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
    @abstractmethod
    def attemptMap(
        self, expression: TExpIn, children_translator: ExpressionVisitor[TExpOut]
    ) -> Optional[TExpOut]:
        raise NotImplementedError


class MappingExpressionTranslator(ExpressionVisitor[TExpOut]):
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
        return self.__map_column(exp, self.__translation_rules.literals)

    def visitColumn(self, exp: Column) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.columns)

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.subscriptables)

    def visitFunctionCall(self, exp: FunctionCall) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.functions)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.curried_functions)

    def visitArgument(self, exp: Argument) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.arguments)

    def visitLambda(self, exp: Lambda) -> TExpOut:
        return self.__map_column(exp, self.__translation_rules.lambdas)

    def __map_column(
        self, exp: TExpIn, rules: Sequence[ExpressionMapper[TExpIn, TExpOut]],
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
