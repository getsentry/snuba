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
    SpansLoader,
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
    SPANS = "spans"


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
    MigrationGroup.SPANS,
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
        loader=SystemLoader(),
        storage_sets_keys={StorageSetKey.MIGRATIONS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.EVENTS: _MigrationGroup(
        loader=EventsLoader(),
        storage_sets_keys={
            StorageSetKey.EVENTS,
            StorageSetKey.EVENTS_RO,
            StorageSetKey.CDC,
        },
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.TRANSACTIONS: _MigrationGroup(
        loader=TransactionsLoader(),
        storage_sets_keys={StorageSetKey.TRANSACTIONS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.DISCOVER: _MigrationGroup(
        loader=DiscoverLoader(),
        storage_sets_keys={StorageSetKey.DISCOVER},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.METRICS: _MigrationGroup(
        loader=MetricsLoader(),
        storage_sets_keys={StorageSetKey.METRICS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.OUTCOMES: _MigrationGroup(
        loader=OutcomesLoader(),
        storage_sets_keys={StorageSetKey.OUTCOMES},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.SESSIONS: _MigrationGroup(
        loader=SessionsLoader(),
        storage_sets_keys={StorageSetKey.SESSIONS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.QUERYLOG: _MigrationGroup(
        loader=QuerylogLoader(),
        storage_sets_keys={StorageSetKey.QUERYLOG},
        readiness_state=ReadinessState.PARTIAL,
    ),
    MigrationGroup.PROFILES: _MigrationGroup(
        loader=ProfilesLoader(),
        storage_sets_keys={StorageSetKey.PROFILES},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.FUNCTIONS: _MigrationGroup(
        loader=FunctionsLoader(),
        storage_sets_keys={StorageSetKey.FUNCTIONS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.REPLAYS: _MigrationGroup(
        loader=ReplaysLoader(),
        storage_sets_keys={StorageSetKey.REPLAYS},
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.GENERIC_METRICS: _MigrationGroup(
        loader=GenericMetricsLoader(),
        storage_sets_keys={
            StorageSetKey.GENERIC_METRICS_SETS,
            StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
            StorageSetKey.GENERIC_METRICS_COUNTERS,
        },
        readiness_state=ReadinessState.COMPLETE,
    ),
    MigrationGroup.TEST_MIGRATION: _MigrationGroup(
        loader=TestMigrationLoader(),
        storage_sets_keys=set(),
        readiness_state=ReadinessState.PARTIAL,
    ),
    MigrationGroup.SEARCH_ISSUES: _MigrationGroup(
        loader=SearchIssuesLoader(),
        storage_sets_keys={StorageSetKey.SEARCH_ISSUES},
        readiness_state=ReadinessState.PARTIAL,
    ),
    MigrationGroup.SPANS: _MigrationGroup(
        loader=SpansLoader(),
        storage_sets_keys={StorageSetKey.SPANS},
        readiness_state=ReadinessState.PARTIAL,
    ),
}


class DuplicateStorageSetFoundInGroup(Exception):
    pass


def build_storage_set_to_group_mapping() -> Dict[StorageSetKey, MigrationGroup]:
    result = {}
    for migration_group, _migration_group in _REGISTERED_MIGRATION_GROUPS.items():
        for storage_set_key in _migration_group.storage_set_keys:
            if storage_set_key in result:
                raise DuplicateStorageSetFoundInGroup(
                    f"The storage_set={storage_set_key} is present in more than one MigrationGroup. Each storage_set should only reference one migration group."
                )
            result[storage_set_key] = migration_group
    return result


_STORAGE_SET_TO_MIGRATION_GROUP_MAPPING: Dict[
    StorageSetKey, MigrationGroup
] = build_storage_set_to_group_mapping()


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_MIGRATION_GROUPS[group].loader


def get_group_readiness_state_from_storage_set(
    storage_set_key: StorageSetKey,
) -> ReadinessState:
    migration_group = _STORAGE_SET_TO_MIGRATION_GROUP_MAPPING[storage_set_key]
    return _REGISTERED_MIGRATION_GROUPS[migration_group].readiness_state


def get_group_readiness_state(group: MigrationGroup) -> ReadinessState:
    return _REGISTERED_MIGRATION_GROUPS[group].readiness_state
