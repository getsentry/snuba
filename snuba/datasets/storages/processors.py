from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from snuba.request.request_settings import RequestSettings

TStorageQuery = TypeVar("TStorageQuery")


class QueryProcessor(ABC, Generic[TStorageQuery]):
    """
    A transformation applied to a storage query. This transformation mutates the
    Query object in place.

    Processors that extend this class are executed during the storage specific part
    of the query execution pipeline.
    As their logical counterparts, Storage query processors are stateless and are
    independent from each other. Each processor must leave the query in a valid state
    and must not depend on the execution of another processor before or after.

    Processors are designed to be stateless. There is no guarantee whether the same
    instance may be reused.
    """

    @abstractmethod
    def process_query(
        self, query: TStorageQuery, request_settings: RequestSettings
    ) -> None:
        # TODO: Make the Query class immutable.
        raise NotImplementedError
