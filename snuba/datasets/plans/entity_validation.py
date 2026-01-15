from __future__ import annotations

from typing import Union

import sentry_sdk

from snuba.datasets.entities.factory import get_entity
from snuba.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException, ValidationException
from snuba.query.logical import EntityQuery
from snuba.query.parser.validation.functions import FunctionCallsValidator
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta

EXPRESSION_VALIDATORS = [FunctionCallsValidator()]


def _validate_query(query: Query) -> None:
    """
    Applies all the expression validators in one pass over the AST.
    """

    for exp in query.get_all_expressions():
        for v in EXPRESSION_VALIDATORS:
            v.validate(exp, query.get_from_clause())


def _validate_entities_with_query(query: Union[CompositeQuery[QueryEntity], EntityQuery]) -> None:
    """
    Applies all validator defined on the entities in the query
    """
    if isinstance(query, EntityQuery):
        entity = get_entity(query.get_from_clause().key)
        try:
            for v in entity.get_validators():
                v.validate(query)
        except InvalidQueryException as e:
            raise ValidationException(
                f"Validation failed for entity {query.get_from_clause().key.value}: {e}",
                should_report=e.should_report,
            )
    else:
        from_clause = query.get_from_clause()
        if isinstance(from_clause, JoinClause):
            alias_map = from_clause.get_alias_node_map()
            for alias, node in alias_map.items():
                assert isinstance(node.data_source, QueryEntity)  # mypy
                entity = get_entity(node.data_source.key)
                try:
                    for v in entity.get_validators():
                        v.validate(query, alias)
                except InvalidQueryException as e:
                    raise ValidationException(
                        f"Validation failed for entity {node.data_source.key.value}: {e}",
                        should_report=e.should_report,
                    )


VALIDATORS = [_validate_query, _validate_entities_with_query]


def run_entity_validators(
    query: Union[CompositeQuery[QueryEntity], EntityQuery],
    settings: QuerySettings | None = None,
) -> None:
    """
    Main function for applying all validators associated with an entity
    """
    for validator_func in VALIDATORS:
        description = getattr(validator_func, "__name__", "custom")
        with sentry_sdk.start_span(op="validator", name=description):
            if settings and settings.get_dry_run():
                with explain_meta.with_query_differ("entity_validator", description, query):
                    validator_func(query)
            else:
                validator_func(query)

    if isinstance(query, CompositeQuery):
        from_clause = query.get_from_clause()
        if isinstance(from_clause, (EntityQuery, CompositeQuery)):
            run_entity_validators(from_clause, settings)
