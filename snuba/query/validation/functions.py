import logging
from typing import Any, Sequence

from snuba.query.expressions import Expression
from snuba.query.functions import is_valid_global_function
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall

logger = logging.getLogger(__name__)


class AllowedFunctionValidator(FunctionCallValidator):
    """
    Validates that the function itself is allowed. This does not validate
    that the function is correctly formed.
    """

    def __init__(self, enforce: bool = False):
        self.__enforce = enforce

    def validate(
        self, func_name: str, parameters: Sequence[Expression], schema: Any
    ) -> None:
        try:
            self.__validate_impl(func_name, parameters, schema)
        except InvalidFunctionCall as exception:
            if self.__enforce:
                raise exception
            else:
                logger.warning(f"Invalid function name: {func_name}", exc_info=True)

    def __validate_impl(
        self, func_name: str, parameters: Sequence[Expression], schema: Any
    ) -> None:
        if not is_valid_global_function(func_name):
            print("func+name", func_name)
            raise InvalidFunctionCall(f"Invalid function name: {func_name}")
