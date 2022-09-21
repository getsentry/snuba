import os
from abc import ABC, abstractmethod

from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class LogicalQueryProcessor(ABC, metaclass=RegisteredClass):
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

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @abstractmethod
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # TODO: Now the query is moved around through the Request object, which
        # is frozen (and it should be), thus the Query itself is mutable since
        # we cannot reassign it.
        # Ideally this should return a query insteadof assuming it mutates the
        # existing one in place. We can move towards an immutable structure
        # after changing Request.
        raise NotImplementedError


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.processors.logical"
)
