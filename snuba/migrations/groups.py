from abc import ABC, abstractmethod
from enum import Enum
from importlib import import_module
from typing import Sequence

from snuba.migrations.migration import Migration


class MigrationGroup(Enum):
    SYSTEM = "system"
    EVENTS = "events"


class GroupLoader(ABC):
    """
    Provides the list of migrations associated with an group and a loader that returns
    the requested migration.
    """

    @abstractmethod
    def get_migrations(self) -> Sequence[str]:
        """
        Returns the list of migration IDs in the order they should be executed.
        """
        raise NotImplementedError

    @abstractmethod
    def load_migration(self, migration_id: str) -> Migration:
        raise NotImplementedError


class DirectoryLoader(GroupLoader):
    """
    Loads migrations that are defined as files of a directory. The file name
    represents the migration ID.
    """

    def __init__(self, module: str) -> None:
        self.__module = module

    def get_migrations(self) -> Sequence[str]:
        return ["0001_migrations"]

    def load_migration(self, migration_id: str) -> Migration:
        module = import_module(f"{self.__module}.{migration_id}")
        return module.Migration()  # type: ignore


_REGISTERED_GROUPS = {
    MigrationGroup.SYSTEM: DirectoryLoader("snuba.migrations.snuba_migrations.system"),
    MigrationGroup.EVENTS: DirectoryLoader("snuba.migrations.snuba_migrations.events"),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_GROUPS[group]
