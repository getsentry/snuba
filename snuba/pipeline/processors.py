import sentry_sdk

from snuba.datasets.entities.factory import get_entity
from snuba.query.logical import EntityQuery
from snuba.query.query_settings import QuerySettings
from snuba.state import explain_meta


def execute_entity_processors(query: EntityQuery, settings: QuerySettings) -> None:
    """
    Executes the entity query processors for the query. These are taken
    from the entity.
    """
    entity = get_entity(query.get_from_clause().key)

    for processor in entity.get_query_processors():
        with sentry_sdk.start_span(
            description=type(processor).__name__, op="processor"
        ):
            if settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "entity_processor", type(processor).__name__, query
                ):
                    processor.process_query(query, settings)
            else:
                processor.process_query(query, settings)
