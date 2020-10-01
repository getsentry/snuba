import logging
from collections import ChainMap
from typing import Mapping

from snuba.clickhouse.columns import Array, String
from snuba.datasets.entity import Entity
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Expression, FunctionCall
from snuba.query.parser.validation import ExpressionValidator
from snuba.query.validation import FunctionCallValidator, InvalidFunctionCall
from snuba.query.validation.signature import Any, Column, SignatureValidator

logger = logging.getLogger(__name__)


default_validators: Mapping[str, FunctionCallValidator] = {
    # like and notLike need to take care of Arrays as well since
    # Arrays are exploded into strings if they are part of the arrayjoin
    # clause.
    # TODO: provide a more restrictive support for arrayjoin.
    "like": SignatureValidator([Column({Array, String}), Any()]),
    "notLike": SignatureValidator([Column({Array, String}), Any()]),
}


class FunctionCallsValidator(ExpressionValidator):
    """
    Applies all function validators on the provided expression.
    The individual function validators are divided in two mappings:
    a default one applied to all queries and one a mapping per dataset.
    """

    def validate(self, exp: Expression, entity: Entity) -> None:
        if not isinstance(exp, FunctionCall):
            return

        entity_validators = entity.get_function_call_validators()
        common_function_validators = (
            entity_validators.keys() & default_validators.keys()
        )
        if common_function_validators:
            logger.warning(
                "Dataset validators are overlapping with default ones. Entity: %s. Overlap %r",
                entity,
                common_function_validators,
                exc_info=True,
            )

        validators = ChainMap(default_validators, entity_validators)
        try:
            # TODO: Decide whether these validators should exist at the Dataset or Entity level
            validator = validators.get(exp.function_name)
            if validator is not None:
                validator.validate(exp.parameters, entity.get_data_model())
        except InvalidFunctionCall as exception:
            raise InvalidExpressionException(
                exp, f"Illegal call to function {exp.function_name}: {str(exception)}",
            ) from exception
