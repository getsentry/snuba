from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Type, TypeVar, Union, cast

from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.datasets.plans.translator.mapper import ExpressionMapper
from snuba.query.dsl import identity
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.matchers import Any
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or
from snuba.query.matchers import String as StringMatch
from snuba.util import qualified_column
from snuba.utils.registered_class import RegisteredClass

TExpIn = TypeVar("TExpIn")
TExpOut = TypeVar("TExpOut")


class SnubaClickhouseMapper(ExpressionMapper[TExpIn, TExpOut, SnubaClickhouseStrictTranslator]):
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


class ColumnMapper(SnubaClickhouseMapper[Column, ValidColumnMappings], metaclass=RegisteredClass):
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
    SnubaClickhouseMapper[CurriedFunctionCall, Union[CurriedFunctionCall, FunctionCall]],
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


@dataclass
class DefaultNoneColumnMapper(ColumnMapper):
    """
    This takes a list of flattened column names and maps them to None (NULL in SQL) as it is done in the discover column_expr method today.
    It should not be used for any other reason or use case, thus it should not be moved out of the discover dataset file.
    """

    column_names: list[str]

    def __post_init__(self) -> None:
        self.columns = set(self.column_names)

    def attempt_map(
        self,
        expression: Column,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if expression.column_name in self.column_names:
            return identity(
                Literal(None, None),
                expression.alias
                or qualified_column(expression.column_name, expression.table_name or ""),
            )
        else:
            return None


@dataclass
class DefaultNoneFunctionMapper(FunctionCallMapper):
    """
    Maps the list of function names to NULL.
    """

    function_names: List[str]

    def __post_init__(self) -> None:
        self.function_match = FunctionCallMatch(
            Or([StringMatch(func) for func in self.function_names])
        )

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if self.function_match.match(expression):
            return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultIfNullFunctionMapper(FunctionCallMapper):
    """
    If a function is being called on a column that doesn't exist, or is being
    called on NULL, change the entire function to be NULL.
    """

    function_match = FunctionCallMatch(
        StringMatch("identity"), (LiteralMatch(value=Any(type(None))),)
    )

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:

        # HACK: Quick fix to avoid this function dropping important conditions from the query
        logical_functions = {"and", "or", "xor"}

        if expression.function_name in logical_functions:
            return None

        parameters = tuple(p.accept(children_translator) for p in expression.parameters)
        for param in parameters:
            # All impossible columns will have been converted to the identity function.
            # So we know that if a function has the identity function as a parameter, we can
            # collapse the entire expression.
            fmatch = self.function_match.match(param)
            if fmatch is not None:
                return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultIfNullCurriedFunctionMapper(CurriedFunctionCallMapper):
    """
    If a curried function is being called on a column that doesn't exist, or is being
    called on NULL, change the entire function to be NULL.
    """

    function_match = FunctionCallMatch(StringMatch("identity"), (LiteralMatch(),))

    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Union[CurriedFunctionCall, FunctionCall]]:
        internal_function = expression.internal_function.accept(children_translator)
        assert isinstance(internal_function, FunctionCall)  # mypy
        parameters = tuple(p.accept(children_translator) for p in expression.parameters)
        for param in parameters:
            # All impossible columns that have been converted to NULL will be the identity function.
            # So we know that if a function has the identity function as a parameter, we can
            # collapse the entire expression.
            fmatch = self.function_match.match(param)
            if fmatch is not None:
                return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultNoneSubscriptMapper(SubscriptableReferenceMapper):
    """
    This maps a subscriptable reference to None (NULL in SQL) as it is done
    in the discover column_expr method today. It should not be used for
    any other reason or use case, thus it should not be moved out of
    the discover dataset file.
    """

    subscript_names: List[str]

    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if expression.column.column_name in self.subscript_names:
            return identity(Literal(None, None), expression.alias)
        else:
            return None
