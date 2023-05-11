from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.clusters.cluster import (
    ClickhouseCluster,
    ClickhouseWriterOptions,
    get_cluster,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.plans.splitters import QuerySplitStrategy
from snuba.datasets.readiness_state import ReadinessState
from snuba.datasets.schemas import Schema
from snuba.datasets.schemas.tables import WritableTableSchema, WriteFormat
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader, TableWriter
from snuba.query.allocation_policies import DEFAULT_PASSTHROUGH_POLICY, AllocationPolicy
from snuba.query.exceptions import QueryPlanException
from snuba.query.processors.condition_checkers import ConditionChecker
from snuba.query.processors.physical import ClickhouseQueryProcessor
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

    def __init__(
        self,
        storage_set_key: StorageSetKey,
        schema: Schema,
        readiness_state: ReadinessState,
    ):
        self.__storage_set_key = storage_set_key
        self.__schema = schema
        self.__readiness_state = readiness_state

    def get_storage_set_key(self) -> StorageSetKey:
        return self.__storage_set_key

    def get_cluster(self, slice_id: Optional[int] = None) -> ClickhouseCluster:
        return get_cluster(self.__storage_set_key, slice_id)

    def get_schema(self) -> Schema:
        return self.__schema

    def get_readiness_state(self) -> ReadinessState:
        return self.__readiness_state


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
    def get_query_processors(self) -> Sequence[ClickhouseQueryProcessor]:
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

    def get_allocation_policy(self) -> AllocationPolicy:
        return DEFAULT_PASSTHROUGH_POLICY


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
        readiness_state: ReadinessState,
        query_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
        query_splitters: Optional[Sequence[QuerySplitStrategy]] = None,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
        allocation_policy: Optional[AllocationPolicy] = None,
    ) -> None:
        self.__storage_key = storage_key
        self.__query_processors = query_processors or []
        self.__query_splitters = query_splitters or []
        self.__mandatory_condition_checkers = mandatory_condition_checkers or []
        self.__allocation_policy = allocation_policy
        super().__init__(storage_set_key, schema, readiness_state)

    def get_storage_key(self) -> StorageKey:
        return self.__storage_key

    def get_query_processors(self) -> Sequence[ClickhouseQueryProcessor]:
        return self.__query_processors

    def get_query_splitters(self) -> Sequence[QuerySplitStrategy]:
        return self.__query_splitters

    def get_mandatory_condition_checkers(self) -> Sequence[ConditionChecker]:
        return self.__mandatory_condition_checkers

    def get_allocation_policy(self) -> AllocationPolicy:
        return self.__allocation_policy or super().get_allocation_policy()


class WritableTableStorage(ReadableTableStorage, WritableStorage):
    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        readiness_state: ReadinessState,
        schema: Schema,
        query_processors: Sequence[ClickhouseQueryProcessor],
        stream_loader: KafkaStreamLoader,
        query_splitters: Optional[Sequence[QuerySplitStrategy]] = None,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
        allocation_policy: Optional[AllocationPolicy] = None,
        replacer_processor: Optional[ReplacerProcessor[Any]] = None,
        writer_options: ClickhouseWriterOptions = None,
        write_format: WriteFormat = WriteFormat.JSON,
        ignore_write_errors: bool = False,
    ) -> None:
        super().__init__(
            storage_key,
            storage_set_key,
            schema,
            readiness_state,
            query_processors,
            query_splitters,
            mandatory_condition_checkers,
            allocation_policy,
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


@dataclass
class EntityStorageConnection:
    storage: ReadableStorage
    translation_mappers: TranslationMappers
    is_writable: bool = False


class EntityStorageConnectionNotFound(Exception):
    pass


class StorageNotAvailable(QueryPlanException):
    pass
