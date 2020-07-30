import logging
from abc import ABC
from typing import Sequence, Set, Type

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.columns import ColumnType as ClickhouseType
from snuba.query.expressions import Expression
from snuba.query.matchers import Any, Column, Param
from snuba.query.parser.exceptions import (
    FunctionValidationException,
    ValidationException,
)
from snuba.query.validation import FunctionCallValidator

logger = logging.getLogger(__name__)


class ParamType(ABC):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        raise NotImplementedError


class AnyType(ParamType):
    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        return


COLUMN_PATTERN = Column(
    alias=None, table_name=None, column_name=Param("column_name", Any(str)),
)


class ColumnType(ParamType):
    """
    Validates the that the type of a Column expression is in a set
    of allowed types.
    """

    def __init__(
        self, types: Set[Type[ClickhouseType]], column_required: bool = False,
    ) -> None:
        self.__valid_types = types
        # If true, this will fail if the expression is not a simple column.
        self.__column_required = column_required

    def validate(self, expression: Expression, schema: ColumnSet) -> None:
        match = COLUMN_PATTERN.match(expression)
        if match is None:
            if self.__column_required:
                raise FunctionValidationException(
                    f"Illegal expression: {expression}. Column required."
                )
            else:
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
        if not isinstance(column_type, tuple(self.__valid_types)):
            raise FunctionValidationException(
                (
                    f"Illegal type {type(column_type)} of argument "
                    f"{column_name}. Required types {self.__valid_types}"
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
        self.__allow_optionals = allow_optionals
        # If False it would simply log invalid functions instead of raising
        # exceptions.
        self.__enforce = enforce

    def validate(self, parameters: Sequence[Expression], schema: ColumnSet) -> None:
        try:
            self.__validate_impl(parameters, schema)
        except ValidationException as exception:
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
            raise FunctionValidationException(
                f"Too few arguments for function call. Required {self.__param_types}"
            )

        if not self.__allow_optionals and len(parameters) > len(self.__param_types):
            raise FunctionValidationException(
                f"Too many arguments for function call. Required {self.__param_types}"
            )

        for validator, param in zip(self.__param_types, parameters):
            validator.validate(param, schema)
