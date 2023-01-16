from abc import ABC, abstractmethod
from typing import List, Type, cast

from snuba.datasets.storage import EntityStorageConnection, ReadableTableStorage
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
        storage_connections: List[EntityStorageConnection],
    ) -> EntityStorageConnection:
        raise NotImplementedError


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
        storage_connections: List[EntityStorageConnection],
    ) -> EntityStorageConnection:
        assert len(storage_connections) == 1
        return storage_connections[0]


class SimpleQueryStorageSelector(QueryStorageSelector):
    """
    A simple query storage selector which selects the storage passed an input.
    """

    def __init__(self, storage: str):
        self.storage_key = StorageKey(storage)

    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: List[EntityStorageConnection],
    ) -> EntityStorageConnection:
        for storage_connection in storage_connections:
            assert isinstance(storage_connection.storage, ReadableTableStorage)
            if storage_connection.storage.get_storage_key() == self.storage_key:
                return storage_connection
        raise QueryStorageSelectorError(
            "The specified storage in selector does not exist in storage list."
        )
