from __future__ import annotations

import os
from abc import ABC, abstractmethod
from glob import glob
from importlib import import_module
from typing import Sequence

from snuba.migrations.errors import MigrationDoesNotExist
from snuba.migrations.migration import Migration


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


class DirectoryLoader(GroupLoader, ABC):
    """
    Loads migrations that are defined as files of a directory. The file name
    represents the migration ID.

    Migrations must be named: xxxx_migration_name.py where xxxx is 4 digit,
    0 padded migration number. As regex: [0-9][0-9][0-9][0-9]_.*\.py
    Within a dir, migration number are strictly increasing by 1 beginning at
    0001
    """

    def __init__(self, module_path: str) -> None:
        self.__module = module_path  # the one with dots not slashes

    def get_migrations(self) -> Sequence[str]:
        """
        Migrations must be in the folder specified by module_path.
        see class comment for migration naming scheme.
        """
        # the folder that the migrations should be in
        migration_folder = self.__module.replace(".", "/")
        if not os.path.exists(migration_folder):
            return []
        # grab the migrations, ignore all other files
        migration_filenames = sorted(
            map(
                lambda x: os.path.basename(x)[:-3],
                glob(os.path.join(migration_folder, "[0-9][0-9][0-9][0-9]_*.py")),
            )
        )
        # validate no duplicate migration numbers
        last = None
        for fname in migration_filenames:
            if last is not None and fname[:4] == last[:4]:
                raise ValueError(
                    f"""Duplicate migration number for the following files:
    {os.path.join(migration_folder,last)}.py
    {os.path.join(migration_folder,fname)}.py"""
                )
            last = fname

        return migration_filenames

    def load_migration(self, migration_id: str) -> Migration:
        try:
            module = import_module(f"{self.__module}.{migration_id}")
            return module.Migration()  # type: ignore
        except ModuleNotFoundError:
            raise MigrationDoesNotExist("Invalid migration ID")


class SystemLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.system_migrations")


class EventsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.events")


class TransactionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.transactions")


class DiscoverLoader(DirectoryLoader):
    """
    This migration group depends on events and transactions
    """

    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.discover")


class OutcomesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.outcomes")


class ReplaysLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.replays")


class MetricsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.metrics")


class SessionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.sessions")


class QuerylogLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.querylog")


class TestMigrationLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.test_migration")


class ProfilesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.profiles")


class FunctionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.functions")


class GenericMetricsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.generic_metrics")


class SearchIssuesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.search_issues")


class SpansLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.spans")


class SpansV2Loader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.spans_v2")


class GroupAttributesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.group_attributes")


class MetricsSummariesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.metrics_summaries")


class ProfileChunksLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.profile_chunks")
