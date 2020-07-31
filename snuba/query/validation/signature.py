import logging
from abc import ABC
from typing import Sequence, Set, Type

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.columns import ColumnType as ClickhouseColumnType
from snuba.clickhouse.columns import Nullable
from snuba.query.expressions import Expression
from snuba.query.matchers import Any as AnyMatcher
from snuba.query.matchers import Column as ColumnMatcher
from snuba.query.matchers import Param
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCallException

logger = logging.getLogger(__name__)


class ParamType(ABC):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        raise NotImplementedError


class Any(ParamType):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        return


COLUMN_PATTERN = ColumnMatcher(
    alias=None, table_name=None, column_name=Param("column_name", AnyMatcher(str)),
)


class Column(ParamType):
    """
    Validates the that the type of a Column expression is in a set
    of allowed types.

    If the expression provided is not a Column, it accepts it.
    We may consider later whether we want to enforce only column
    expressions can be passed as arguments in certain functions.
    """

    def __init__(
        self, types: Set[Type[ClickhouseColumnType]], nullable: bool = True
    ) -> None:
        self.__valid_types = types
        self.__allow_nullable = nullable

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

        column_type = column.type.get_raw()
        nullable = Nullable in column.type.get_all_modifiers()
        if not isinstance(column_type, tuple(self.__valid_types)) or (
            nullable and not self.__allow_nullable
        ):
            raise InvalidFunctionCallException(
                (
                    f"Illegal type {'Nullable' if nullable else ''} {type(column_type)} "
                    f"of argument {column_name}. Required types {self.__valid_types}"
                )
            )


class SignatureValidator(FunctionCallValidator):
    """
    Validates the signature of the function call.
    """

    def __init__(
        self,
        param_types: Sequence[ParamType],
        allow_optionals: bool = False,
        enforce: bool = True,
    ):
        self.__param_types = param_types
        # Allow optionals, if True, lets the validator accept functions with
        # an arbitrary number of extra parameters after those it validates.
        # It would validate the function call against the sequence of parameters
        # the validator is instantiated with and accept anything else beyond
        # such sequence.
        self.__allow_optionals = allow_optionals
        # If False it would simply log invalid functions instead of raising
        # exceptions.
        self.__enforce = enforce

    def validate(self, parameters: Sequence[Expression], schema: ColumnSet) -> None:
        try:
            self.__validate_impl(parameters, schema)
        except InvalidFunctionCallException as exception:
            if self.__enforce:
                raise exception
            else:
                logger.warning(
                    f"Query validation exception. Validator: {self}", exc_info=True
                )

    def __validate_impl(
        self, parameters: Sequence[Expression], schema: ColumnSet
    ) -> None:
        if len(parameters) < len(self.__param_types):
            raise InvalidFunctionCallException(
                f"Too few arguments for function call. Required {self.__param_types}"
            )

        if not self.__allow_optionals and len(parameters) > len(self.__param_types):
            raise InvalidFunctionCallException(
                f"Too many arguments for function call. Required {self.__param_types}"
            )

        for validator, param in zip(self.__param_types, parameters):
            validator.validate(param, schema)
