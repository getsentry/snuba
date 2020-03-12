from abc import ABC
from typing import Optional, Sequence


from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.table_storage import TableWriter
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


class Storage(ABC):
    """
    Storage is an abstraction on anything we can run a query onto in our
    database. This means that it generally represents a Clickhouse table
    or a view.
    It provides:
    - what we need to build the query (the schemas)
    - it can provide a table writer if we can write on this storage
    - a sequence of query processors that are applied to the query after
      the storage is selected.
    There are one or multiple storages per dataset (in the future, there
    will be multiple per entity). During the query processing a storage
    is selected and the query focuses on that storage from that point.
    """

    # TODO: Break StorageSchemas apart. It contains a distinction between write schema and
    # read schema that existed before this dataset model and before TableWriters (then we
    # trusted StorageSchemas to define which schema we would write on and which one we would
    # read from). This is not needed anymore since TableWriter has its own write schema.
    def get_schemas(self) -> StorageSchemas:
        """
        Returns the collections of schemas for DDL operations and for query.
        See TableWriter to get a write schema.
        """
        raise NotImplementedError

    def can_write(self) -> bool:
        """
        Returns True if this dataset has write capabilities
        """
        return self.get_table_writer() is not None

    def get_table_writer(self) -> Optional[TableWriter]:
        """
        Returns the TableWriter if the Storage has one.
        """
        raise NotImplementedError

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        """
        Returns a series of transformation functions (in the form of QueryProcessor objects)
        that are applied to queries after parsing and before running them on Clickhouse.
        These are applied in sequence in the same order as they are defined and are supposed
        to be stateless.
        """
        raise NotImplementedError


class TableStorage(Storage):
    """
    A table storage that represents either a table or a view.
    """

    def __init__(
        self,
        schemas: StorageSchemas,
        table_writer: Optional[TableWriter] = None,
        query_processors: Optional[Sequence[QueryProcessor]] = None,
    ) -> None:
        self.__schemas = schemas
        self.__table_writer = table_writer
        self.__query_processors = query_processors or []

    def get_schemas(self) -> StorageSchemas:
        return self.__schemas

    def get_table_writer(self) -> Optional[TableWriter]:
        return self.__table_writer

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return self.__query_processors


class QueryStorageSelector(ABC):
    """
    The component provided by a dataset and used at the beginning of the
    execution of a query to pick the storage query should be executed onto.
    """

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> Storage:
        raise NotImplementedError


class SingleTableQueryStorageSelector(QueryStorageSelector):
    def __init__(self, storage: TableStorage) -> None:
        self.__storage = storage

    def select_storage(
        self, query: Query, request_settings: RequestSettings
    ) -> Storage:
        return self.__storage
