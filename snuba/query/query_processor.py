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
    return a processed query object.
    """

    @abstractmethod
    def process_query(self,
        query: Query,
        context_data: TQueryProcessContext,
    ) -> Query:
        raise NotImplementedError


class ExtensionQueryProcessor(QueryProcessor[ExtensionData]):
    """
    Common parent class for all the extension processors. The only
    contribution of this class is to resolve the generic context to
    extension data. So subclasses of this one can be used to process
    query extensions.
    """

    @abstractmethod
    def process_query(self, query: Query, extension_data: ExtensionData) -> Query:
        raise NotImplementedError


class DummyExtensionProcessor(ExtensionQueryProcessor):

    def process_query(self, query: Query, extension_data: ExtensionData) -> Query:
        return query
