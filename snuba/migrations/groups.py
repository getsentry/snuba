from enum import Enum

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
    def __init__(self, loader: GroupLoader, readiness_state: ReadinessState) -> None:
        self.loader = loader
        self.readiness_state = readiness_state


_REGISTERED_MIGRATION_GROUPS = {
    MigrationGroup.SYSTEM: _MigrationGroup(SystemLoader(), ReadinessState.COMPLETE),
    MigrationGroup.EVENTS: _MigrationGroup(EventsLoader(), ReadinessState.COMPLETE),
    MigrationGroup.TRANSACTIONS: _MigrationGroup(
        TransactionsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.DISCOVER: _MigrationGroup(DiscoverLoader(), ReadinessState.COMPLETE),
    MigrationGroup.METRICS: _MigrationGroup(MetricsLoader(), ReadinessState.COMPLETE),
    MigrationGroup.OUTCOMES: _MigrationGroup(OutcomesLoader(), ReadinessState.COMPLETE),
    MigrationGroup.SESSIONS: _MigrationGroup(SessionsLoader(), ReadinessState.COMPLETE),
    MigrationGroup.QUERYLOG: _MigrationGroup(QuerylogLoader(), ReadinessState.PARTIAL),
    MigrationGroup.PROFILES: _MigrationGroup(ProfilesLoader(), ReadinessState.COMPLETE),
    MigrationGroup.FUNCTIONS: _MigrationGroup(
        FunctionsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.REPLAYS: _MigrationGroup(ReplaysLoader(), ReadinessState.COMPLETE),
    MigrationGroup.GENERIC_METRICS: _MigrationGroup(
        GenericMetricsLoader(), ReadinessState.COMPLETE
    ),
    MigrationGroup.TEST_MIGRATION: _MigrationGroup(
        TestMigrationLoader(), ReadinessState.LIMITED
    ),
    MigrationGroup.SEARCH_ISSUES: _MigrationGroup(
        SearchIssuesLoader(), ReadinessState.PARTIAL
    ),
    MigrationGroup.SPANS: _MigrationGroup(SpansLoader(), ReadinessState.LIMITED),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_MIGRATION_GROUPS[group].loader
