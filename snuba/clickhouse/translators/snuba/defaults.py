from copy import deepcopy
from typing import Optional

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
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)


class DefaultLiteralMapper(LiteralMapper):
    def attempt_map(
        self, expression: Literal, children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Literal]:
        return deepcopy(expression)


class DefaultColumnMapper(ColumnMapper):
    def attempt_map(
        self, expression: Column, children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Column]:
        return deepcopy(expression)


class DefaultSubscriptableMapper(SubscriptableReferenceMapper):
    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[SubscriptableReference]:
        # TODO: remove a default for SubscriptableReference entirely. Since there is not
        # SubscriptableReference in clickhouse, such columns have to be translated by
        # a valid rule. They cannot have a default translation that makes a copy.
        # The assertion will be removed as well.
        column = expression.column.accept(children_translator)
        assert isinstance(column, Column)
        key = expression.key.accept(children_translator)
        assert isinstance(key, Literal)
        return SubscriptableReference(alias=expression.alias, column=column, key=key)


class DefaultFunctionMapper(FunctionCallMapper):
    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        return FunctionCall(
            alias=expression.alias,
            function_name=expression.function_name,
            parameters=tuple(
                p.accept(children_translator) for p in expression.parameters
            ),
        )


class DefaultCurriedFunctionMapper(CurriedFunctionCallMapper):
    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[CurriedFunctionCall]:
        return CurriedFunctionCall(
            alias=expression.alias,
            internal_function=children_translator.translate_function_strict(
                expression.internal_function
            ),
            parameters=tuple(
                p.accept(children_translator) for p in expression.parameters
            ),
        )


class DefaultArgumentMapper(ArgumentMapper):
    def attempt_map(
        self,
        expression: Argument,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Argument]:
        return deepcopy(expression)


class DefaultLambdaMapper(LambdaMapper):
    def attempt_map(
        self, expression: Lambda, children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Lambda]:
        return Lambda(
            alias=expression.alias,
            parameters=expression.parameters,
            transformation=expression.transformation.accept(children_translator),
        )
