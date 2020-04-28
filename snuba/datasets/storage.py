from abc import ABC, abstractmethod
from typing import Optional, Sequence

from snuba.clusters.cluster import ClickhouseCluster, get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import TableWriter
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class Storage(ABC):
    """
    Storage is an abstraction that represent a DB object that stores data
    and has a schema.
    There are one or multiple storages per dataset (in the future, there
    will be multiple per entity). During the query processing a storage
    is selected and the query focuses on that storage from that point.

    By itself, Storage, does not do much. See the subclasses
    for more useful abstractions.
    """

    def __init__(self, storage_key: StorageKey, storage_set_key: StorageSetKey):
        self.__storage_key = storage_key
        self.__storage_set_key = storage_set_key

    def get_storage_key(self) -> StorageKey:
        return self.__storage_key

    def get_storage_set_key(self) -> StorageSetKey:
        return self.__storage_set_key

    def get_cluster(self) -> ClickhouseCluster:
        return get_cluster(self.__storage_set_key)

    # TODO: Break StorageSchemas apart. It contains a distinction between write schema and
    # read schema that existed before this dataset model and before TableWriters (then we
    # trusted StorageSchemas to define which schema we would write on and which one we would
    # read from). This is not needed anymore since TableWriter is provided the correct write
    # schema through the constructor.
    @abstractmethod
    def get_schemas(self) -> StorageSchemas:
        """
        Returns the collections of schemas for DDL operations and for query.
        See TableWriter to get a write schema.
        """
        raise NotImplementedError


class ReadableStorage(Storage):
    """
    ReadableStorage is an abstraction on anything we can run a query onto in our
    database. This means that it generally represents a Clickhouse table
    or a view.
    It provides:
    - what we need to build the query (the schemas)
    - a sequence of query processors that are applied to the query after
      the storage is selected.
    """

    @abstractmethod
    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on Clickhouse.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        raise NotImplementedError


class WritableStorage(Storage):
    """
    WritableStorage is an abstraction on anything we can write onto on the
    database. This means that it generally represents a Clickhouse table
    and it provides a writer to actually perform the writes.
    """

    @abstractmethod
    def get_table_writer(self) -> TableWriter:
        """
        Returns the TableWriter if the Storage has one.
        """
        raise NotImplementedError


class ReadableTableStorage(ReadableStorage):
    """
    A table storage that represents either a table or a view.
    """

    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        schemas: StorageSchemas,
        query_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        self.__schemas = schemas
        self.__query_processors = query_processors or []
        super().__init__(storage_key, storage_set_key)

    def get_schemas(self) -> StorageSchemas:
        return self.__schemas

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return self.__query_processors


class WritableTableStorage(ReadableTableStorage, WritableStorage):
    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        schemas: StorageSchemas,
        table_writer: TableWriter,
        query_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        super().__init__(storage_key, storage_set_key, schemas, query_processors)
        self.__table_writer = table_writer

    def get_table_writer(self) -> TableWriter:
        return self.__table_writer


class QueryStorageSelector(ABC):
    """
    The component provided by a dataset and used at the beginning of the
    execution of a query to pick the storage query should be executed onto.
    """

    @abstractmethod
    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> ReadableStorage:
        raise NotImplementedError
