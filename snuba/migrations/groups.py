from __future__ import annotations

from enum import Enum

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


class ReadinessState(Enum):
    LIMITED = "limited"
    DEPRECATE = "deprecate"
    PARTIAL = "partial"
    COMPLETE = "complete"


class _MigrationGroup:
    def __init__(
        self, value: str, loader: GroupLoader, readiness_state: ReadinessState
    ) -> None:
        self.value = value
        self.loader = loader
        self.readiness_state = readiness_state


_REGISTERED_MIGRATION_GROUPS = {
    MigrationGroup.SYSTEM: _MigrationGroup(
        "system", SystemLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.EVENTS: _MigrationGroup(
        "events", EventsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.TRANSACTIONS: _MigrationGroup(
        "transactions", TransactionsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.DISCOVER: _MigrationGroup(
        "discover", DiscoverLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.METRICS: _MigrationGroup(
        "metrics", MetricsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.OUTCOMES: _MigrationGroup(
        "outcomes", OutcomesLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.SESSIONS: _MigrationGroup(
        "sessions", SessionsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.QUERYLOG: _MigrationGroup(
        "querylog", QuerylogLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.PROFILES: _MigrationGroup(
        "profiles", ProfilesLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.FUNCTIONS: _MigrationGroup(
        "functions", FunctionsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.REPLAYS: _MigrationGroup(
        "replays", ReplaysLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.GENERIC_METRICS: _MigrationGroup(
        "generic_metrics", GenericMetricsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.TEST_MIGRATION: _MigrationGroup(
        "test_migration", TestMigrationLoader(), ReadinessState.LIMITED
    ),
    MigrationGroup.SEARCH_ISSUES: _MigrationGroup(
        "search_issues", SearchIssuesLoader(), ReadinessState.PARTIAL
    ),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_MIGRATION_GROUPS[group].loader
