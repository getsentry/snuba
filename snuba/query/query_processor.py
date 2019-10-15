from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings

TQueryProcessContext = TypeVar("TQueryProcessContext")


class QueryExtensionProcessor(ABC, Generic[TQueryProcessContext]):
    """
    Base class for all query processors. Whatever extends this
    class is supposed to provide one method that takes a query
    object of type Query that represent the parsed query to
    process, a context (which depends on the processor) and
    updates it.
    """

    @abstractmethod
    def process_query(self,
        query: Query,
        context_data: TQueryProcessContext,
        request_settings: RequestSettings,
    ) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


class DummyExtensionProcessor(QueryExtensionProcessor[Any]):

    def process_query(self, query: Query, extension_data: Any) -> None:
        pass
