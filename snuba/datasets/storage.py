from abc import ABC, abstractmethod
from typing import Any, NamedTuple, Optional, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.cluster import (
    ClickhouseCluster,
    ClickhouseWriterOptions,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.split_strategy import QuerySplitStrategy
from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import WritableTableSchema, WriteFormat
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader, TableWriter
from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.replacers.replacer_processor import ReplacerProcessor


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

    def __init__(self, storage_set_key: StorageSetKey, schema: Schema):
        self.__storage_set_key = storage_set_key
        self.__schema = schema

    def get_storage_set_key(self) -> StorageSetKey:
        return self.__storage_set_key

    def get_cluster(self) -> ClickhouseCluster:
        return get_cluster(self.__storage_set_key)

    def get_schema(self) -> Schema:
        return self.__schema


class ConditionChecker(ABC):
    """
    Checks if an expression matches a specific shape and content.

    These are declared by storages as mandatory conditions that are
    supposed to be in the query before it is executed for the query
    to be acceptable.

    This system is meant to be a failsafe mechanism to prevent
    bugs in any step of query processing to generate queries that are
    missing project_id and org_id conditions from the query.
    """

    @abstractmethod
    def get_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def check(self, expression: Expression) -> bool:
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

    def get_query_splitters(self) -> Sequence[QuerySplitStrategy]:
        """
        If this storage supports splitting queries as optimizations, they are provided here.
        These are optimizations, the query plan builder may decide to override the storage
        and to skip the splitters. So correctness of the query must not depend on these
        strategies to be applied.
        """
        return []

    def get_mandatory_condition_checkers(self) -> Sequence[ConditionChecker]:
        """
        Returns a list of expression patterns that need to always be
        present in the query before executing it on Clickhouse.
        The kind of patterns we expect here is meant to ensure conditions
        like "project_id = something" are never missing right before
        sending the query to Clickhouse.
        """
        return []


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
        schema: Schema,
        query_processors: Optional[Sequence[QueryProcessor]] = None,
        query_splitters: Optional[Sequence[QuerySplitStrategy]] = None,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
    ) -> None:
        self.__storage_key = storage_key
        self.__query_processors = query_processors or []
        self.__query_splitters = query_splitters or []
        self.__mandatory_condition_checkers = mandatory_condition_checkers or []
        super().__init__(storage_set_key, schema)

    def get_storage_key(self) -> StorageKey:
        return self.__storage_key

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return self.__query_processors

    def get_query_splitters(self) -> Sequence[QuerySplitStrategy]:
        return self.__query_splitters

    def get_mandatory_condition_checkers(self) -> Sequence[ConditionChecker]:
        return self.__mandatory_condition_checkers


class WritableTableStorage(ReadableTableStorage, WritableStorage):
    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        schema: Schema,
        query_processors: Sequence[QueryProcessor],
        stream_loader: KafkaStreamLoader,
        query_splitters: Optional[Sequence[QuerySplitStrategy]] = None,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
        replacer_processor: Optional[ReplacerProcessor[Any]] = None,
        writer_options: ClickhouseWriterOptions = None,
        write_format: WriteFormat = WriteFormat.JSON,
        ignore_write_errors: bool = False,
    ) -> None:
        super().__init__(
            storage_key,
            storage_set_key,
            schema,
            query_processors,
            query_splitters,
            mandatory_condition_checkers,
        )
        assert isinstance(schema, WritableTableSchema)
        self.__table_writer = TableWriter(
            storage_set=storage_set_key,
            write_schema=schema,
            stream_loader=stream_loader,
            replacer_processor=replacer_processor,
            writer_options=writer_options,
            write_format=write_format,
        )
        self.__ignore_write_errors = ignore_write_errors

    def get_table_writer(self) -> TableWriter:
        return self.__table_writer

    def get_is_write_error_ignorable(self) -> bool:
        return self.__ignore_write_errors


class StorageAndMappers(NamedTuple):
    storage: ReadableStorage
    mappers: TranslationMappers


class QueryStorageSelector(ABC):
    """
    The component provided by a dataset and used at the beginning of the
    execution of a query to pick the storage query should be executed onto.
    """

    @abstractmethod
    def select_storage(
        self, query: Query, query_settings: QuerySettings
    ) -> StorageAndMappers:
        raise NotImplementedError
