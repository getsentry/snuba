from typing import Any, Mapping

from snuba.query import Query
from snuba.query.logical import Query as LogicalQuery
from snuba.request.request_settings import RequestSettings
from snuba.datasets.entities.factory import get_entity

import sentry_sdk


def legacy_request_preprocessor(
    extensions: Mapping[str, Mapping[str, Any]], query: Query, settings: RequestSettings
) -> None:
    """
    Query pre processor for the legacy Snuba query language.
    Such language has query extensions so they have to be
    processed. That can be done since we have only simple
    queries in the legacy language.
    """

    assert isinstance(query, LogicalQuery)
    query_entity = query.get_from_clause()
    entity = get_entity(query_entity.key)

    extensions_processors = entity.get_extensions()
    for name, extension in extensions_processors.items():
        with sentry_sdk.start_span(
            description=type(extension.get_processor()).__name__, op="extension"
        ):
            extension.get_processor().process_query(query, extensions[name], settings)


def noop_request_processor(query: Query, settings: RequestSettings) -> None:
    pass
