from abc import ABC, abstractmethod
from typing import Callable, MutableMapping, Type

from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings


class QueryProcessor(ABC):
    """
    A transformation applied to a Query. This depends on the query structure and
    on the request.query_settings. No additional context is provided.
    This transformation mutates the Query object in place.

    These processors tweak the query and are developed independently from each other,
    thus they have to be fully independent.
    When developing a processor for a dataset, it would be a bad idea to implicitly
    depend on the result of a previous one. Each processor should always check its
    own preconditions instead.

    Processors are designed to be stateless. There is no guarantee whether the same
    instance may be reused.
    """

    @abstractmethod
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


_SEEN_QUERY_PROCESSORS: MutableMapping[str, Type[QueryProcessor]] = {}

# Decorator for registering your query processor in the static list
def query_processor(
    name: str,
) -> Callable[[Type[QueryProcessor]], Type[QueryProcessor]]:
    def register_query_processor(cls: Type[QueryProcessor]) -> Type[QueryProcessor]:
        assert (
            _SEEN_QUERY_PROCESSORS.get(name) is None
        ), f"{cls} trying to claim name {name}, already owned by {_SEEN_QUERY_PROCESSORS[name]}"
        _SEEN_QUERY_PROCESSORS[name] = cls
        return cls

    return register_query_processor


def get_query_processor_by_name(name: str) -> Type[QueryProcessor]:
    return _SEEN_QUERY_PROCESSORS[name]
