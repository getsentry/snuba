from enum import Enum
from typing import Dict, Set

from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.readiness_state import ReadinessState
from snuba.migrations.group_loader import (
    DiscoverLoader,
    EventsLoader,
    FunctionsLoader,
    GenericMetricsLoader,
    GroupLoader,
    MetricsLoader,
    OutcomesLoader,
    ProfilesLoader,
    QuerylogLoader,
    ReplaysLoader,
    SearchIssuesLoader,
    SessionsLoader,
    SystemLoader,
    TestMigrationLoader,
    TransactionsLoader,
)


class MigrationGroup(Enum):
    SYSTEM = "system"
    EVENTS = "events"
    TRANSACTIONS = "transactions"
    DISCOVER = "discover"
    OUTCOMES = "outcomes"
    METRICS = "metrics"
    SESSIONS = "sessions"
    QUERYLOG = "querylog"
    PROFILES = "profiles"
    FUNCTIONS = "functions"
    REPLAYS = "replays"
    GENERIC_METRICS = "generic_metrics"
    TEST_MIGRATION = "test_migration"
    SEARCH_ISSUES = "search_issues"


# Migration groups are mandatory by default. Specific groups can
# only be skipped (SKIPPED_MIGRATION_GROUPS) if the exist in this list.
OPTIONAL_GROUPS = {
    MigrationGroup.METRICS,
    MigrationGroup.SESSIONS,
    MigrationGroup.QUERYLOG,
    MigrationGroup.PROFILES,
    MigrationGroup.FUNCTIONS,
    MigrationGroup.REPLAYS,
    MigrationGroup.GENERIC_METRICS,
    MigrationGroup.TEST_MIGRATION,
    MigrationGroup.SEARCH_ISSUES,
}


class _MigrationGroup:
    def __init__(
        self,
        loader: GroupLoader,
        storage_sets_keys: Set[StorageSetKey],
        readiness_state: ReadinessState,
    ) -> None:
        self.loader = loader
        self.storage_set_keys = storage_sets_keys
        self.readiness_state = readiness_state


_REGISTERED_MIGRATION_GROUPS: Dict[MigrationGroup, _MigrationGroup] = {
    MigrationGroup.SYSTEM: _MigrationGroup(
        SystemLoader(), {StorageSetKey.MIGRATIONS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.EVENTS: _MigrationGroup(
        EventsLoader(),
        {StorageSetKey.EVENTS, StorageSetKey.EVENTS_RO, StorageSetKey.CDC},
        ReadinessState.COMPLETE,
    ),
    MigrationGroup.TRANSACTIONS: _MigrationGroup(
        TransactionsLoader(), {StorageSetKey.TRANSACTIONS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.DISCOVER: _MigrationGroup(
        DiscoverLoader(), {StorageSetKey.DISCOVER}, ReadinessState.COMPLETE
    ),
    MigrationGroup.METRICS: _MigrationGroup(
        MetricsLoader(), {StorageSetKey.METRICS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.OUTCOMES: _MigrationGroup(
        OutcomesLoader(), {StorageSetKey.OUTCOMES}, ReadinessState.COMPLETE
    ),
    MigrationGroup.SESSIONS: _MigrationGroup(
        SessionsLoader(), {StorageSetKey.SESSIONS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.QUERYLOG: _MigrationGroup(
        QuerylogLoader(), {StorageSetKey.QUERYLOG}, ReadinessState.PARTIAL
    ),
    MigrationGroup.PROFILES: _MigrationGroup(
        ProfilesLoader(), {StorageSetKey.PROFILES}, ReadinessState.COMPLETE
    ),
    MigrationGroup.FUNCTIONS: _MigrationGroup(
        FunctionsLoader(), {StorageSetKey.FUNCTIONS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.REPLAYS: _MigrationGroup(
        ReplaysLoader(), {StorageSetKey.REPLAYS}, ReadinessState.COMPLETE
    ),
    MigrationGroup.GENERIC_METRICS: _MigrationGroup(
        GenericMetricsLoader(),
        {
            StorageSetKey.GENERIC_METRICS_SETS,
            StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
            StorageSetKey.GENERIC_METRICS_COUNTERS,
        },
        ReadinessState.COMPLETE,
    ),
    MigrationGroup.TEST_MIGRATION: _MigrationGroup(
        TestMigrationLoader(), set(), ReadinessState.LIMITED
    ),
    MigrationGroup.SEARCH_ISSUES: _MigrationGroup(
        SearchIssuesLoader(), {StorageSetKey.SEARCH_ISSUES}, ReadinessState.PARTIAL
    ),
}


class DuplicateStorageSetFoundInGroup(Exception):
    pass


def build_storage_set_to_migration_group_mapping() -> Dict[
    StorageSetKey, MigrationGroup
]:
    result = {}
    for migration_group, _migration_group in _REGISTERED_MIGRATION_GROUPS.items():
        for storage_set_key in _migration_group.storage_set_keys:
            if storage_set_key in result:
                raise DuplicateStorageSetFoundInGroup(
                    f"The storage_set={storage_set_key} is present in more than one MigrationGroup. Each storage_set should only reference one migration group."
                )
            result[storage_set_key] = migration_group
    return result


STORAGE_SET_TO_MIGRATION_GROUP_MAPPING: Dict[
    StorageSetKey, MigrationGroup
] = build_storage_set_to_migration_group_mapping()


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_MIGRATION_GROUPS[group].loader


def get_group_readiness_state_from_storage_set(
    storage_set_key: StorageSetKey,
) -> ReadinessState:
    migration_group = STORAGE_SET_TO_MIGRATION_GROUP_MAPPING[storage_set_key]
    return _REGISTERED_MIGRATION_GROUPS[migration_group].readiness_state
