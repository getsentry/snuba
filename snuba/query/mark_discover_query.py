from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class MarkDiscoverQuery(QueryProcessor):
    """
    Temporary query processor to support accessing transactions through the events storage.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        query.set_hint("discover")
