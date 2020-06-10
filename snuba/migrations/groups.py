from abc import ABC, abstractmethod
from enum import Enum
from importlib import import_module

from snuba.migrations.migration import Migration


class MigrationGroup(Enum):
    SYSTEM = "system"
    SNUBA = "snuba"


class GroupLoader(ABC):
    """
    Provides the list of migrations associated with an group and a loader that returns
    the requested migration.
    """

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

    def load_migration(self, migration_id: str) -> Migration:
        module = import_module(f"{self.__module}.{migration_id}")
        return module.Migration()  # type: ignore


_REGISTERED_GROUPS = {
    MigrationGroup.SYSTEM: DirectoryLoader("snuba.migrations.snuba_migrations.system"),
    MigrationGroup.SNUBA: DirectoryLoader("snuba.migrations.snuba_migrations.snuba"),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_GROUPS[group]
