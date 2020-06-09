from abc import ABC, abstractmethod
from importlib import import_module

from snuba.migrations.migration import App, Migration


class AppLoader(ABC):
    """
    Provides the list of migrations associated with an app and a loader that returns
    the requested migration.
    """

    @abstractmethod
    def load_migration(self, migration_id: str) -> Migration:
        raise NotImplementedError


class DirectoryLoader(AppLoader):
    """
    Loads migrations that are defined as files of a directory. The file name
    represents the migration ID.
    """

    def __init__(self, module: str):
        self.__module = module

    def load_migration(self, migration_id: str) -> Migration:
        module = import_module(f"{self.__module}.{migration_id}")
        return module.Migration()  # type: ignore


_REGISTERED_APPS = {
    App.SYSTEM: DirectoryLoader("snuba.migrations.system"),
    App.SNUBA: DirectoryLoader("snuba.migrations.snuba"),
}


def get_app(app: App) -> AppLoader:
    return _REGISTERED_APPS[app]
