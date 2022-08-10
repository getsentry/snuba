from abc import ABC, abstractmethod
from typing import Any, Mapping, MutableMapping, Optional, Type

from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings

_QUERY_PROCESSOR_REGISTRY: MutableMapping[str, Type["QueryProcessor"]] = {}


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

    def __init_subclass__(
        cls: Type["QueryProcessor"],
        config_name: Optional[str] = None,
        **kwargs: Mapping[str, Any],
    ) -> None:
        if config_name:
            assert (
                _QUERY_PROCESSOR_REGISTRY.get(config_name) is None
            ), f"{cls} trying to claim name {config_name}, already owned by {_QUERY_PROCESSOR_REGISTRY[config_name]}"
            _QUERY_PROCESSOR_REGISTRY[config_name] = cls

        return super().__init_subclass__(**kwargs)  # type: ignore

    @abstractmethod
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


def get_query_processor_by_name(name: str) -> Type[QueryProcessor]:
    return _QUERY_PROCESSOR_REGISTRY[name]
