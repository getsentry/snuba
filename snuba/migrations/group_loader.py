from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
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
        files = sorted(
            filter(
                lambda f: os.path.isfile(os.path.join(migration_folder, f)),
                os.listdir(migration_folder),
            )
        )
        migration_filenames = []
        pattern = re.compile(r"[0-9][0-9][0-9][0-9]_.*\.py")
        for f in files:
            if re.fullmatch(pattern, f):
                migration_filenames.append(f[:-3])

        # validate the migration numbers are strictly increasing starting at 0001
        for i, fname in enumerate(migration_filenames):
            if not (int(fname[:4]) == i + 1):
                raise ValueError(
                    f"Migrations in folder {migration_folder} were not formatted as expected. Expected migration number {str(i+1).zfill(4)} for migration {fname} but got {fname[:4]}"
                )

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


class GroupAttributesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.group_attributes")


class MetricsSummariesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.metrics_summaries")


class ProfileChunksLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.profile_chunks")
