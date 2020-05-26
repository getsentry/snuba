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


# Below are the abstract classes that need to be extended in order to
# write a mapping rule SnubaClickhouseMappingTranslator would accept.
# Nothing broader that does not fit in these types constraints is
# allowed.


class LiteralMapper(SnubaClickhouseMapper[Literal, Literal]):
    """
    Literals can only be translated into Literals.
    """

    pass


class ColumnMapper(SnubaClickhouseMapper[Column, Union[Column, Literal, FunctionCall]]):
    """
    Columns can be translated into other Columns, or Literals (if a Snuba
    column has a hardcoded value on a storage like the event type on
    transactions), or FunctionCalls (common when wrapping Columns into
    assertNotNull calls).
    """

    pass


class FunctionCallMapper(SnubaClickhouseMapper[FunctionCall, FunctionCall]):
    """
    Functions are only allowed to become Functions so that we can ensure
    CurriedFunctions internal functions can be successfully translated.

    TODO: We actually need to loosen this constraint (for functions
    translating into pre aggregated Column). Though before doing so we
    need to give a dedicated type that will be stricter for the
    CurriedFunction internal function.
    """

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
    """
    A SubscriptableReference can only translate into a FunctionCall.
    There cannot be a SubscriptableReference concept in the Clickhouse
    AST.
    We temporarily allow to translate it into SubscriptableReference until
    the translator is wired up and subscriptable column actually have
    a translator.
    """

    pass


class LambdaMapper(SnubaClickhouseMapper[Lambda, Lambda]):
    pass


class ArgumentMapper(SnubaClickhouseMapper[Argument, Argument]):
    pass
