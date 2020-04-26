from abc import ABC, abstractmethod

from snuba.clickhouse.query import Query
from snuba.request.request_settings import RequestSettings


class QueryProcessor(ABC):
    """
    This class represents the same concept as the Logical QueryProcessor (see
    query.processors.logical).
    The only difference is that it works on the Physical Query instead of the
    Logical Query.
    """

    @abstractmethod
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # TODO: Make the Query class immutable.
        raise NotImplementedError
