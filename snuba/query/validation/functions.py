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

        if is_valid_global_function(func_name):
            return

        if self.__enforce:
            raise InvalidFunctionCall(f"Invalid function name: {func_name}")
        else:
            logger.warning(f"Invalid function name: {func_name}", exc_info=True)
