import logging
from abc import ABC
from datetime import date, datetime
from typing import Sequence, Set, Type, Union

from snuba.clickhouse.columns import (
    UUID,
    Array,
    Date,
    DateTime,
    FixedString,
    Float,
    IPv4,
    IPv6,
    Nullable,
    String,
    UInt,
)
from snuba.query.data_source import DataSource
from snuba.query.expressions import Expression
from snuba.query.expressions import Literal as LiteralType
from snuba.query.matchers import Any as AnyMatcher
from snuba.query.matchers import Column as ColumnMatcher
from snuba.query.matchers import Param
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall
from snuba.utils.schemas import ColumnSet

logger = logging.getLogger(__name__)


class ParamType(ABC):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        raise NotImplementedError


class Any(ParamType):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        return

    def __str__(self) -> str:
        return "Any"


COLUMN_PATTERN = ColumnMatcher(
    table_name=None,
    column_name=Param("column_name", AnyMatcher(str)),
)

AllowedTypes = Union[
    Type[Array],
    Type[String],
    Type[UUID],
    Type[IPv4],
    Type[IPv6],
    Type[FixedString],
    Type[UInt],
    Type[Float],
    Type[Date],
    Type[DateTime],
]

AllowedScalarTypes = Union[
    Type[None],
    Type[bool],
    Type[str],
    Type[float],
    Type[int],
    Type[date],
    Type[datetime],
]


class Column(ParamType):
    """
    Validates that the type of a Column expression is in a set of
    allowed types.

    If the expression provided is not a Column, it accepts it.
    We may consider later whether we want to enforce only column
    expressions can be passed as arguments in certain functions.

    This class discriminates between Nullable columns and non Nullable.
    If the allow_nullable field, is True this will accept both, if it
    is False it will require non nullable columns.
    """

    def __init__(self, types: Set[AllowedTypes], allow_nullable: bool = True) -> None:
        self.__valid_types = types
        self.__allow_nullable = allow_nullable

    def __str__(self) -> str:
        return f"{'Nullable ' if self.__allow_nullable else ''}{self.__valid_types}"

    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        match = COLUMN_PATTERN.match(expression)
        if match is None:
            return

        column_name = match.string("column_name")
        column = schema.get(column_name)
        if column is None:
            # TODO: We cannot raise exceptions if the column is not present
            # on the schema just yet because the current logical schemas are
            # sadly not complete. Fix them and then raise an exception in this
            # case.
            return

        nullable = column.type.has_modifier(Nullable)

        if not isinstance(column.type, tuple(self.__valid_types)) or (
            nullable and not self.__allow_nullable
        ):
            raise InvalidFunctionCall(
                (
                    f"Illegal type {'Nullable ' if nullable else ''}{str(column.type)} "
                    f"of argument `{column_name}`. Required types {self.__valid_types}"
                )
            )


class Literal(ParamType):
    """
    Validates that the type of a Literal expression is in a set of
    allowed types.

    If the expression provided is not a Literal, it accepts it.
    We may consider later whether we want to enforce only literal
    expressions can be passed as arguments in certain functions.
    """

    def __init__(
        self, types: Set[AllowedScalarTypes], allow_nullable: bool = False
    ) -> None:
        self.__valid_types = types
        if allow_nullable:
            self.__valid_types.add(type(None))

    def __str__(self) -> str:
        return f"{self.__valid_types}"

    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        if not isinstance(expression, LiteralType):
            return None

        value = expression.value
        if not isinstance(value, tuple(self.__valid_types)):
            raise InvalidFunctionCall(
                f"Illegal type {type(value)} of argument {value}. Required types {self.__valid_types}"
            )


class SignatureValidator(FunctionCallValidator):
    """
    Validates the signature of the function call.
    The signature is defined as a sequence of ParamType objects.
    """

    def __init__(
        self,
        param_types: Sequence[ParamType],
        allow_extra_params: bool = False,
        enforce: bool = True,
    ):
        self.__param_types = param_types
        # If True, this signature allows extra parameters after those
        # specified by param_types. The extra parameters are not
        # validated.
        self.__allow_extra_params = allow_extra_params
        # If False it would simply log invalid functions instead of raising
        # exceptions.
        self.__enforce = enforce

    def validate(
        self, func_name: str, parameters: Sequence[Expression], data_source: DataSource
    ) -> None:
        try:
            self.__validate_impl(func_name, parameters, data_source)
        except InvalidFunctionCall as exception:
            if self.__enforce:
                raise exception
            else:
                logger.warning(
                    f"Query validation exception. Validator: {self}", exc_info=True
                )

    def __validate_impl(
        self, func_name: str, parameters: Sequence[Expression], data_source: DataSource
    ) -> None:
        if len(parameters) < len(self.__param_types):
            raise InvalidFunctionCall(
                f"Too few arguments. Required {[str(t) for t in self.__param_types]}"
            )

        if not self.__allow_extra_params and len(parameters) > len(self.__param_types):
            raise InvalidFunctionCall(
                f"Too many arguments. Required {[str(t) for t in self.__param_types]}"
            )

        for validator, param in zip(self.__param_types, parameters):
            validator.validate(param, data_source.get_columns())
