import os
from abc import ABC, abstractmethod
from typing import Sequence, Type, cast

from snuba.datasets.storage import EntityStorageConnection
from snuba.query.logical import Query
from snuba.query.query_settings import QuerySettings
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory


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
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        raise NotImplementedError


import_submodules_in_directory(
    os.path.dirname(os.path.realpath(__file__)),
    "snuba.datasets.entities.storage_selectors",
)
