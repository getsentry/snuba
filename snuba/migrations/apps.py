from abc import ABC, abstractmethod
from importlib import import_module

from snuba.migrations.migration import App, Migration


class AppLoader(ABC):
    @abstractmethod
    def load_migration(self, migration_id: str) -> Migration:
        raise NotImplementedError


class DirectoryLoader(AppLoader):
    def __init__(self, module: str):
        self.__module = module

    def load_migration(self, migration_id: str) -> Migration:
        module = import_module(f"snuba.migrations.{self.__module}.{migration_id}")
        return module.Migration()  # type: ignore


_REGISTERED_APPS = {
    App.SYSTEM: DirectoryLoader("system"),
    App.SNUBA: DirectoryLoader("snuba"),
}


def get_app(app: App) -> AppLoader:
    return _REGISTERED_APPS[app]
