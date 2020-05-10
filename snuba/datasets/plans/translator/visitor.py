from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Sequence, TypeVar

from snuba.datasets.plans.translator.mapping_rules import (
    SimpleExpressionMappingRule,
    StructuredExpressionMappingRule,
)
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
class ExpressionMappingSpec(Generic[TExpOut]):
    literals: Sequence[SimpleExpressionMappingRule[Literal, TExpOut]]
    columns: Sequence[SimpleExpressionMappingRule[Column, TExpOut]]
    subscriptables: Sequence[
        StructuredExpressionMappingRule[SubscriptableReference, TExpOut]
    ]
    functions: Sequence[StructuredExpressionMappingRule[FunctionCall, TExpOut]]
    curried_functions: Sequence[
        StructuredExpressionMappingRule[CurriedFunctionCall, TExpOut]
    ]
    arguments: Sequence[SimpleExpressionMappingRule[Argument, TExpOut]]
    lambdas: Sequence[StructuredExpressionMappingRule[Lambda, TExpOut]]

    def concat(
        self, spec: ExpressionMappingSpec[TExpOut]
    ) -> ExpressionMappingSpec[TExpOut]:
        return ExpressionMappingSpec(
            literals=[*self.literals, *spec.literals],
            columns=[*self.columns, *spec.columns],
            subscriptables=[*self.subscriptables, *spec.subscriptables],
            functions=[*self.functions, *spec.functions],
            curried_functions=[*self.curried_functions, *spec.curried_functions],
            arguments=[*self.arguments, *spec.arguments],
            lambdas=[*self.lambdas, *spec.lambdas],
        )


TExpIn = TypeVar("TExpIn", bound=Expression)


class MappingExpressionTranslator(ExpressionVisitor[TExpOut]):
    def __init__(self, mapping_specs: ExpressionMappingSpec[TExpOut]):
        self.__mapping_specs = mapping_specs

    def visitLiteral(self, exp: Literal) -> TExpOut:
        return self.__map_simple_column(exp, self.__mapping_specs.literals)

    def visitColumn(self, exp: Column) -> TExpOut:
        return self.__map_simple_column(exp, self.__mapping_specs.columns)

    def visitSubscriptableReference(self, exp: SubscriptableReference) -> TExpOut:
        return self.__map_structured_column(exp, self.__mapping_specs.subscriptables)

    def visitFunctionCall(self, exp: FunctionCall) -> TExpOut:
        return self.__map_structured_column(exp, self.__mapping_specs.functions)

    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> TExpOut:
        return self.__map_structured_column(exp, self.__mapping_specs.curried_functions)

    def visitArgument(self, exp: Argument) -> TExpOut:
        return self.__map_simple_column(exp, self.__mapping_specs.arguments)

    def visitLambda(self, exp: Lambda) -> TExpOut:
        return self.__map_structured_column(exp, self.__mapping_specs.lambdas)

    def __map_simple_column(
        self, exp: TExpIn, rules: Sequence[SimpleExpressionMappingRule[TExpIn, TExpOut]]
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")

    def __map_structured_column(
        self,
        exp: TExpIn,
        rules: Sequence[StructuredExpressionMappingRule[TExpIn, TExpOut]],
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
