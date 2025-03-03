from __future__ import annotations

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
from snuba.datasets.deletion_settings import DeletionSettings
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
        required_time_column: Optional[str] = None,
    ):
        self.__storage_set_key = storage_set_key
        self.__schema = schema
        self.__readiness_state = readiness_state
        self.__required_time_column = required_time_column

    def get_storage_set_key(self) -> StorageSetKey:
        return self.__storage_set_key

    def get_cluster(self, slice_id: Optional[int] = None) -> ClickhouseCluster:
        return get_cluster(self.__storage_set_key, slice_id)

    def get_schema(self) -> Schema:
        return self.__schema

    def get_readiness_state(self) -> ReadinessState:
        return self.__readiness_state

    @property
    def required_time_column(self) -> str | None:
        return self.__required_time_column


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

    def get_mandatory_condition_checkers(self) -> Sequence[ConditionChecker]:
        """
        Returns a list of expression patterns that need to always be
        present in the query before executing it on Clickhouse.
        The kind of patterns we expect here is meant to ensure conditions
        like "project_id = something" are never missing right before
        sending the query to Clickhouse.
        """
        return []

    @abstractmethod
    def get_storage_key(self) -> StorageKey:
        raise NotImplementedError

    def get_allocation_policies(self) -> list[AllocationPolicy]:
        return [DEFAULT_PASSTHROUGH_POLICY]


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
        deletion_settings: Optional[DeletionSettings] = None,
        deletion_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
        allocation_policies: Optional[list[AllocationPolicy]] = None,
        delete_allocation_policies: Optional[list[AllocationPolicy]] = None,
        required_time_column: Optional[str] = None,
    ) -> None:
        self.__storage_key = storage_key
        self.__query_processors = query_processors or []
        self.__deletion_settings = deletion_settings or DeletionSettings(0, [], [], 0)
        self.__deletion_processors = deletion_processors or []
        self.__mandatory_condition_checkers = mandatory_condition_checkers or []
        self.__allocation_policies = allocation_policies or []
        self.__delete_allocation_policies = delete_allocation_policies or []
        super().__init__(
            storage_set_key,
            schema,
            readiness_state,
            required_time_column=required_time_column,
        )

    def get_storage_key(self) -> StorageKey:
        return self.__storage_key

    def get_query_processors(self) -> Sequence[ClickhouseQueryProcessor]:
        return self.__query_processors

    def get_mandatory_condition_checkers(self) -> Sequence[ConditionChecker]:
        return self.__mandatory_condition_checkers

    def get_allocation_policies(self) -> list[AllocationPolicy]:
        return self.__allocation_policies or super().get_allocation_policies()

    def get_delete_allocation_policies(self) -> list[AllocationPolicy]:
        return self.__delete_allocation_policies

    def get_deletion_settings(self) -> DeletionSettings:
        return self.__deletion_settings

    def get_deletion_processors(self) -> Sequence[ClickhouseQueryProcessor]:
        return self.__deletion_processors


class WritableTableStorage(ReadableTableStorage, WritableStorage):
    def __init__(
        self,
        storage_key: StorageKey,
        storage_set_key: StorageSetKey,
        readiness_state: ReadinessState,
        schema: Schema,
        query_processors: Sequence[ClickhouseQueryProcessor],
        stream_loader: KafkaStreamLoader,
        mandatory_condition_checkers: Optional[Sequence[ConditionChecker]] = None,
        allocation_policies: Optional[list[AllocationPolicy]] = None,
        delete_allocation_policies: Optional[list[AllocationPolicy]] = None,
        replacer_processor: Optional[ReplacerProcessor[Any]] = None,
        deletion_settings: Optional[DeletionSettings] = None,
        deletion_processors: Optional[Sequence[ClickhouseQueryProcessor]] = None,
        writer_options: ClickhouseWriterOptions = None,
        write_format: WriteFormat = WriteFormat.JSON,
        ignore_write_errors: bool = False,
        required_time_column: Optional[str] = None,
    ) -> None:
        self.__storage_key = storage_key
        super().__init__(
            storage_key,
            storage_set_key,
            schema,
            readiness_state,
            query_processors,
            deletion_settings,
            deletion_processors,
            mandatory_condition_checkers,
            allocation_policies,
            delete_allocation_policies,
            required_time_column=required_time_column,
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

    def get_storage_key(self) -> StorageKey:
        return self.__storage_key

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
