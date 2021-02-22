import logging
from collections import ChainMap
from typing import Mapping, Optional

from snuba.clickhouse.columns import Array, ColumnSet, String
from snuba.datasets.entities.factory import get_entity
from snuba.query import Query
from snuba.query.data_source import DataSource
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.join import JoinClause
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

    def validate(self, exp: Expression, data_source: DataSource) -> None:
        if not isinstance(exp, FunctionCall):
            return

        entity_validators: Mapping[str, FunctionCallValidator] = {}
        data_model: Optional[ColumnSet] = None
        if isinstance(data_source, QueryEntity):
            entity = get_entity(data_source.key)
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
            data_model = entity.get_data_model()
        elif isinstance(data_source, (Query, JoinClause)):
            data_model = data_source.get_columns()
        else:
            return

        validators = ChainMap(default_validators, entity_validators)
        try:
            # TODO: Decide whether these validators should exist at the Dataset or Entity level
            validator = validators.get(exp.function_name)
            if validator is not None:
                validator.validate(exp.parameters, data_model)
        except InvalidFunctionCall as exception:
            raise InvalidExpressionException(
                exp, f"Illegal call to function {exp.function_name}: {str(exception)}",
            ) from exception
