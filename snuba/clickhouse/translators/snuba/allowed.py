from typing import Type, TypeVar, Union, cast

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
from snuba.utils.registered_class import RegisteredClass

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


ValidColumnMappings = Union[Column, Literal, FunctionCall, CurriedFunctionCall]


class ColumnMapper(
    SnubaClickhouseMapper[Column, ValidColumnMappings], metaclass=RegisteredClass
):
    """
    Columns can be translated into other Columns, or Literals (if a Snuba
    column has a hardcoded value on a storage like the event type on
    transactions), or FunctionCalls (common when wrapping Columns into
    assertNotNull calls).
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["ColumnMapper"]:
        return cast(Type["ColumnMapper"], cls.class_from_name(name))


class FunctionCallMapper(
    SnubaClickhouseMapper[FunctionCall, FunctionCall], metaclass=RegisteredClass
):
    """
    Functions are only allowed to become Functions so that we can ensure
    CurriedFunctions internal functions can be successfully translated.

    TODO: We actually need to loosen this constraint since we will have
    cases of functions that have to be translated into columns when we
    rely on pre-aggregated tables. Though, before doing so, we need to give
    a dedicated type to the CurriedFunction internal function since that
    requires a function to be translated into a function as of now.
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["FunctionCallMapper"]:
        return cast(Type["FunctionCallMapper"], cls.class_from_name(name))


class CurriedFunctionCallMapper(
    SnubaClickhouseMapper[
        CurriedFunctionCall, Union[CurriedFunctionCall, FunctionCall]
    ],
    metaclass=RegisteredClass,
):
    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["CurriedFunctionCallMapper"]:
        return cast(Type["CurriedFunctionCallMapper"], cls.class_from_name(name))


class SubscriptableReferenceMapper(
    SnubaClickhouseMapper[
        SubscriptableReference,
        Union[FunctionCall, Literal, SubscriptableReference],
    ],
    metaclass=RegisteredClass,
):
    """
    A SubscriptableReference can only translate into a FunctionCall.
    There cannot be a SubscriptableReference concept in the Clickhouse
    AST.
    We temporarily allow to translate it into SubscriptableReference until
    the translator is wired up and subscriptable column actually have
    a translator.
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["SubscriptableReferenceMapper"]:
        return cast(Type["SubscriptableReferenceMapper"], cls.class_from_name(name))


class LambdaMapper(SnubaClickhouseMapper[Lambda, Lambda]):
    pass


class ArgumentMapper(SnubaClickhouseMapper[Argument, Argument]):
    pass
