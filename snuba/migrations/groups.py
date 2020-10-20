from abc import ABC, abstractmethod
from enum import Enum
from importlib import import_module
from typing import Sequence

from snuba import settings
from snuba.migrations.errors import MigrationDoesNotExist
from snuba.migrations.migration import Migration


class MigrationGroup(Enum):
    SYSTEM = "system"
    EVENTS = "events"
    TRANSACTIONS = "transactions"
    OUTCOMES = "outcomes"
    SESSIONS = "sessions"
    QUERYLOG = "querylog"
    SPANS_EXPERIMENTAL = "spans_experimental"


# Migration groups are mandatory by default, unless they are on this list
OPTIONAL_GROUPS = {
    MigrationGroup.SESSIONS,
    MigrationGroup.QUERYLOG,
    MigrationGroup.SPANS_EXPERIMENTAL,
}

ACTIVE_MIGRATION_GROUPS = [
    group
    for group in MigrationGroup
    if not (
        group in OPTIONAL_GROUPS and group.value in settings.SKIPPED_MIGRATION_GROUPS
    )
]


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
    """

    def __init__(self, module: str) -> None:
        self.__module = module

    @abstractmethod
    def get_migrations(self) -> Sequence[str]:
        raise NotImplementedError

    def load_migration(self, migration_id: str) -> Migration:
        try:
            module = import_module(f"{self.__module}.{migration_id}")
            return module.Migration()  # type: ignore
        except ModuleNotFoundError:
            raise MigrationDoesNotExist("Invalid migration ID")


class SystemLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.system")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_migrations"]


class EventsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.events")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_events_initial",
            "0002_events_onpremise_compatibility",
            "0003_errors",
            "0004_errors_onpremise_compatibility",
            "0005_events_tags_hash_map",
            "0006_errors_tags_hash_map",
            "0007_groupedmessages",
            "0008_groupassignees",
            "0009_errors_add_http_fields",
            "0010_groupedmessages_onpremise_compatibility",
            "0011_rebuild_errors",
        ]


class TransactionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.transactions")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_transactions",
            "0002_transactions_onpremise_fix_orderby_and_partitionby",
            "0003_transactions_onpremise_fix_columns",
            "0004_transactions_add_tags_hash_map",
            "0005_transactions_add_measurements",
            "0006_transactions_add_http_fields",
        ]


class OutcomesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.outcomes")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_outcomes", "0002_outcomes_remove_size_and_bytes"]


class SessionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.sessions")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_sessions"]


class QuerylogLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.querylog")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_querylog", "0002_status_type_change", "0003_add_profile_fields"]


class SpansExperimentalLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.spans_experimental")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_spans_experimental"]


_REGISTERED_GROUPS = {
    MigrationGroup.SYSTEM: SystemLoader(),
    MigrationGroup.EVENTS: EventsLoader(),
    MigrationGroup.TRANSACTIONS: TransactionsLoader(),
    MigrationGroup.OUTCOMES: OutcomesLoader(),
    MigrationGroup.SESSIONS: SessionsLoader(),
    MigrationGroup.QUERYLOG: QuerylogLoader(),
    MigrationGroup.SPANS_EXPERIMENTAL: SpansExperimentalLoader(),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_GROUPS[group]
