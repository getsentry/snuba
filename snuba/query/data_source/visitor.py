from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Union

from snuba.query import ProcessableQuery, TSimpleDataSource
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import JoinClause
from snuba.query.data_source.multi import MultiQuery

TReturn = TypeVar("TReturn")


class DataSourceVisitor(ABC, Generic[TReturn, TSimpleDataSource]):
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
            TSimpleDataSource,
            JoinClause[TSimpleDataSource],
            ProcessableQuery[TSimpleDataSource],
            CompositeQuery[TSimpleDataSource],
            MultiQuery[TSimpleDataSource],
        ],
    ) -> TReturn:
        if isinstance(data_source, JoinClause):
            return self._visit_join(data_source)
        elif isinstance(data_source, ProcessableQuery):
            return self._visit_simple_query(data_source)
        elif isinstance(data_source, CompositeQuery):
            return self._visit_composite_query(data_source)
        elif isinstance(data_source, MultiQuery):
            return self._visit_multi_query(data_source)
        else:
            # It must be a simple data source according to the type
            # signature, we cannot do that via the isinstance call
            # since that type does not exist at runtime.
            return self._visit_simple_source(data_source)

    @abstractmethod
    def _visit_simple_source(self, data_source: TSimpleDataSource) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_join(self, data_source: JoinClause[TSimpleDataSource]) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_simple_query(
        self, data_source: ProcessableQuery[TSimpleDataSource]
    ) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_composite_query(
        self, data_source: CompositeQuery[TSimpleDataSource]
    ) -> TReturn:
        raise NotImplementedError

    @abstractmethod
    def _visit_multi_query(self, data_source: MultiQuery[TSimpleDataSource]) -> TReturn:
        raise NotImplementedError
