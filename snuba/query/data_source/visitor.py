from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Union

from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.simple import SimpleDataSource

TReturn = TypeVar("TReturn")


class DataSourceVisitor(ABC, Generic[TReturn]):
    """
    Visitor like system to process a tree of data sources.

    Unfortunately this cannot be a proper visitor, where each class
    has its own `accept` method because these classes are in different
    modules. Building a visitor, by definition, introduces a circular
    dependency thus that is not viable if we want to preserve static
    type checking (we want).
    """

    def visit(
        self,
        data_source: Union[
            SimpleDataSource,
            JoinClause,
            ProcessableQuery,
            CompositeQuery,
        ],
    ) -> TReturn:
        if isinstance(data_source, JoinClause):
            return self._visit_join(data_source)
        elif isinstance(data_source, ProcessableQuery):
            return self._visit_simple_query(data_source)
        elif isinstance(data_source, CompositeQuery):
            return self._visit_composite_query(data_source)
        else:
            # It must be a simple data source according to the type
            # signature, we cannot do that via the isinstance call
            # since that type does not exist at runtime.
            return self._visit_simple_source(data_source)

    @abstractmethod
    def _visit_simple_source(self, data_source: SimpleDataSource) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_join(self, data_source: JoinClause) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_simple_query(self, data_source: ProcessableQuery) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_composite_query(self, data_source: CompositeQuery) -> TReturn:
        raise NotImplementedError
