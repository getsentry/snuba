import logging
from collections import ChainMap
from typing import Mapping

from snuba.datasets.dataset import Dataset
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.parser.exceptions import ValidationException
from snuba.query.parser.validation import ExpressionValidator
from snuba.query.validation import FunctionCallValidator
from snuba.state import get_config

logger = logging.getLogger(__name__)

default_validators: Mapping[str, FunctionCallValidator] = {}


class FunctionCallsValidator(ExpressionValidator):
    def validate(self, exp: Expression, dataset: Dataset) -> None:
        if not isinstance(exp, FunctionCall):
            return

        dataset_validators = dataset.get_function_call_validators()
        common_function_validators = (
            dataset_validators.keys() & default_validators.keys()
        )
        if common_function_validators:
            logger.warning(
                "Dataset validators are overlapping with default ones. Dataset: %s. Overlap %r",
                dataset,
                common_function_validators,
                exc_info=True,
            )

        validators = ChainMap(default_validators, dataset_validators)
        try:
            validator = validators.get(exp.function_name)
            if validator is not None:
                validator.validate(exp.parameters, dataset.get_abstract_columnset())
        except ValidationException as exception:
            if get_config("enforce_expression_validation", 0):
                raise exception
            else:
                logger.warning("Query validation exception", exc_info=True)
