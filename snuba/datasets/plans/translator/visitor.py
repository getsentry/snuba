from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Optional, Sequence, TypeVar

from snuba.datasets.plans.translator.mapping_rules import (
    SimpleExpressionMapper,
    StructuredExpressionMapper,
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
    literals: Sequence[SimpleExpressionMapper[Literal, TExpOut]]
    columns: Sequence[SimpleExpressionMapper[Column, TExpOut]]
    subscriptables: Sequence[
        StructuredExpressionMapper[SubscriptableReference, TExpOut]
    ]
    functions: Sequence[StructuredExpressionMapper[FunctionCall, TExpOut]]
    curried_functions: Sequence[
        StructuredExpressionMapper[CurriedFunctionCall, TExpOut]
    ]
    arguments: Sequence[SimpleExpressionMapper[Argument, TExpOut]]
    lambdas: Sequence[StructuredExpressionMapper[Lambda, TExpOut]]

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
    def __init__(
        self,
        mapping_specs: ExpressionMappingSpec[TExpOut],
        default_rules: Optional[ExpressionMappingSpec[TExpOut]],
    ) -> None:
        self.__mapping_specs = (
            mapping_specs if not default_rules else mapping_specs.concat(default_rules)
        )

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
        self, exp: TExpIn, rules: Sequence[SimpleExpressionMapper[TExpIn, TExpOut]]
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")

    def __map_structured_column(
        self, exp: TExpIn, rules: Sequence[StructuredExpressionMapper[TExpIn, TExpOut]],
    ) -> TExpOut:
        for r in rules:
            ret = r.attemptMap(exp, self)
            if ret is not None:
                return ret
        raise ValueError(f"Cannot map expression {exp}")
