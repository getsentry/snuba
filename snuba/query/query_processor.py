from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, TypeVar

from snuba.query.query import Query

ExtensionData = Mapping[str, Any]

TQueryProcessContext = TypeVar("TQueryProcessContext")


class QueryProcessor(ABC, Generic[TQueryProcessContext]):
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
        request_settings: Mapping[str, bool],
        query_hints: Mapping[str, Any],  # used to pass hints from the extension processors to the clickhouse query
    ) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


class ExtensionQueryProcessor(QueryProcessor[ExtensionData]):
    """
    Common parent class for all the extension processors. The only
    contribution of this class is to resolve the generic context to
    extension data. So subclasses of this one can be used to process
    query extensions.
    """

    @abstractmethod
    def process_query(
            self, query: Query,
            extension_data: ExtensionData,
            request_settings: Mapping[str, bool],
            query_hints: Mapping[str, Any],
    ) -> None:
        raise NotImplementedError


class DummyExtensionProcessor(ExtensionQueryProcessor):

    def process_query(
            self,
            query: Query,
            extension_data: ExtensionData,
            request_settings: Mapping[str, bool],
            query_hints: Mapping[str, Any],
    ) -> None:
        pass
