import copy

from abc import ABC, abstractmethod

from snuba.query.physical import PhysicalQuery
from snuba.query.query import Query


class QueryTranslator(ABC):
    """
    This is a placeholder interface to identify the component that will translate
    the Logical Query into the Physical Query.
    The implementations will evolve when we will provide storage specific translations,
    multi-table storages, and a different query ast between the Logical and Physical
    Query.
    """

    @abstractmethod
    def translate(self, query: Query) -> PhysicalQuery:
        raise NotImplementedError


class CopyTranslator(QueryTranslator):
    """
    The simplest possible translator. It just ensures that we are not using the same
    Query object anymore, so it forces the storage query processing to work on a
    different object than the Logical Query
    """

    def translate(self, query: Query) -> PhysicalQuery:
        return copy.deepcopy(query)
