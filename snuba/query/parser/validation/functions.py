import logging
from collections import ChainMap
from typing import Mapping, Optional

from snuba.clickhouse.columns import Array, String
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.entity import Entity
from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source import DataSource
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity as QueryEntity
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


def _get_entity(data_source: DataSource) -> Optional[Entity]:
    """
        Handles getting the entity depending on the data source.
    """
    if isinstance(data_source, QueryEntity):
        return get_entity(data_source.key)

    elif isinstance(data_source, JoinClause):
        alias_map = data_source.get_alias_node_map()
        for _, node in alias_map.items():
            # just taking the first entity for now, will validate all coming up
            assert isinstance(node.data_source, QueryEntity)  # mypy
            return get_entity(node.data_source.key)
        return None

    elif isinstance(data_source, ProcessableQuery):
        return get_entity(data_source.get_from_clause().key)

    elif isinstance(data_source, CompositeQuery):
        # from clauses for composite queries are either a
        # join clause, processable query, or another composite
        # query, so recursively call until we get the join clause
        # or processable query
        return _get_entity(data_source.get_from_clause())

    else:
        return None


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
        entity: Optional[Entity] = None

        entity = _get_entity(data_source)
        if not entity:
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
            if validator is not None and entity is not None:
                validator.validate(exp.parameters, entity)
        except InvalidFunctionCall as exception:
            raise InvalidExpressionException(
                exp,
                f"Illegal call to function {exp.function_name}: {str(exception)}",
                report=False,
            ) from exception
