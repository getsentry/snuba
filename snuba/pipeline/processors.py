from sentry_sdk import traces

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
        with traces.start_span(
            name=type(processor).__name__, attributes={"sentry.op": "processor"}
        ):
            if settings.get_dry_run():
                with explain_meta.with_query_differ(
                    "entity_processor", type(processor).__name__, query
                ):
                    processor.process_query(query, settings)
            else:
                processor.process_query(query, settings)
