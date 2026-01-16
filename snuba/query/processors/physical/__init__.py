import os
from abc import ABC, abstractmethod
from typing import cast

from snuba.clickhouse.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.query_settings import QuerySettings
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


class ClickhouseQueryProcessor(ABC, metaclass=RegisteredClass):
    """
    A transformation applied to a Clickhouse Query. This transformation mutates the
    Query object in place.

    Processors that extend this class are executed during the Clickhouse specific part
    of the query execution pipeline.
    As their logical counterparts, Clickhouse query processors are stateless and are
    independent from each other. Each processor must leave the query in a valid state
    and must not depend on the execution of another processor before or after.

    Processors are designed to be stateless. There is no guarantee whether the same
    instance will be reused.
    """

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> "ClickhouseQueryProcessor":
        return cls(**kwargs)

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @abstractmethod
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # TODO: Consider making the Query immutable.
        raise NotImplementedError

    @classmethod
    def get_from_name(cls, name: str) -> "ClickhouseQueryProcessor":
        return cast("ClickhouseQueryProcessor", cls.class_from_name(name))


class CompositeQueryProcessor(ABC):
    """
    A transformation applied to a Clickhouse Composite Query.
    This transformation mutates the Query object in place.

    The same rules defined above for QueryProcessor still apply:
    - composite query processor are stateless
    - they are independent from each other
    - they must keep the query in a valid state.
    """

    @abstractmethod
    def process_query(self, query: CompositeQuery[Table], query_settings: QuerySettings) -> None:
        raise NotImplementedError


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)), "snuba.query.processors.physical"
)
