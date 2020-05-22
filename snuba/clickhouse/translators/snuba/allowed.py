from typing import TypeVar, Union

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.datasets.plans.translator.mapper import ExpressionMapper
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)

TExpIn = TypeVar("TExpIn")
TExpOut = TypeVar("TExpOut")


class SnubaClickhouseMapper(
    ExpressionMapper[TExpIn, TExpOut, SnubaClickhouseStrictTranslator]
):
    pass


class LiteralMapper(SnubaClickhouseMapper[Literal, Literal]):
    pass


class ColumnMapper(SnubaClickhouseMapper[Column, Union[Column, Literal, FunctionCall]]):
    pass


class FunctionCallMapper(SnubaClickhouseMapper[FunctionCall, FunctionCall]):
    pass


class CurriedFunctionCallMapper(
    SnubaClickhouseMapper[CurriedFunctionCall, CurriedFunctionCall]
):
    pass


class SubscriptableReferenceMapper(
    SnubaClickhouseMapper[
        SubscriptableReference, Union[FunctionCall, SubscriptableReference],
    ],
):
    pass


class LambdaMapper(SnubaClickhouseMapper[Lambda, Lambda]):
    pass


class ArgumentMapper(SnubaClickhouseMapper[Argument, Argument]):
    pass
