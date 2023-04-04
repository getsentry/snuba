from typing import Sequence

from snuba import environment, state
from snuba.query.data_source import DataSource
from snuba.query.expressions import Expression
from snuba.query.functions import is_valid_global_function
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "validation.functions")


class AllowedFunctionValidator(FunctionCallValidator):
    """
    Validates that the function itself is allowed. This does not validate
    that the function is correctly formed.
    """

    def validate(
        self, func_name: str, parameters: Sequence[Expression], data_source: DataSource
    ) -> None:

        if is_valid_global_function(func_name):
            return

        if state.get_config("function-validator.enabled", False):
            raise InvalidFunctionCall(f"Invalid function name: {func_name}")
        else:
            metrics.increment("invalid_funcs", tags={"func_name": func_name})
