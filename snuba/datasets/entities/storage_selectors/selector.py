from abc import ABC, abstractmethod
from typing import List, Sequence, Type, cast

from snuba.datasets.storage import (
    ReadableTableStorage,
    StorageAndMappers,
    StorageAndMappersNotFound,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.utils.registered_class import RegisteredClass


class QueryStorageSelectorError(Exception):
    pass


class QueryStorageSelector(ABC, metaclass=RegisteredClass):
    """
    The component provided by a dataset and used at the beginning of the
    execution of a query to pick the storage query should be executed onto.
    """

    @classmethod
    def config_key(cls) -> str:
        return cls.__name__

    @classmethod
    def get_from_name(cls, name: str) -> Type["QueryStorageSelector"]:
        return cast(Type["QueryStorageSelector"], cls.class_from_name(name))

    @abstractmethod
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_and_mappers: List[StorageAndMappers],
    ) -> StorageAndMappers:
        raise NotImplementedError

    def get_storage_mapping_pair(
        self,
        storage_key: StorageKey,
        storage_and_mappers: Sequence[StorageAndMappers],
    ) -> StorageAndMappers:
        for sm_tuple in storage_and_mappers:
            assert isinstance(sm_tuple.storage, ReadableTableStorage)
            if storage_key == sm_tuple.storage.get_storage_key():
                return sm_tuple
        raise StorageAndMappersNotFound(
            f"Unable to find storage and translation mappers pair for {storage_key}"
        )


class DefaultQueryStorageSelector(QueryStorageSelector):
    """
    A default query storage selector which chooses the only storage specified in config.
    Entities which define multiple storages should not use this selector and should use
    custom ones.
    """

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_and_mappers: List[StorageAndMappers],
    ) -> StorageAndMappers:
        assert len(storage_and_mappers) == 1
        return storage_and_mappers[0]
