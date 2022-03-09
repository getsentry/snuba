from abc import ABC, abstractmethod
from enum import Enum
from importlib import import_module
from typing import Sequence

from snuba.migrations.errors import MigrationDoesNotExist
from snuba.migrations.migration import Migration


class MigrationGroup(Enum):
    SYSTEM = "system"
    EVENTS = "events"
    TRANSACTIONS = "transactions"
    DISCOVER = "discover"
    OUTCOMES = "outcomes"
    METRICS = "metrics"
    SESSIONS = "sessions"
    QUERYLOG = "querylog"
    SPANS_EXPERIMENTAL = "spans_experimental"
    PROFILES = "profiles"


# Migration groups are mandatory by default, unless they are on this list
OPTIONAL_GROUPS = {
    MigrationGroup.METRICS,
    MigrationGroup.SESSIONS,
    MigrationGroup.QUERYLOG,
    MigrationGroup.SPANS_EXPERIMENTAL,
    MigrationGroup.PROFILES,
}


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
            "0012_errors_make_level_nullable",
            "0013_errors_add_hierarchical_hashes",
            "0014_backfill_errors",
            "0015_truncate_events",
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
            "0007_transactions_add_discover_cols",
            "0008_transactions_add_timestamp_index",
            "0009_transactions_fix_title_and_message",
            "0010_transactions_nullable_trace_id",
            "0011_transactions_add_span_op_breakdowns",
            "0012_transactions_add_spans",
            "0013_transactions_reduce_spans_exclusive_time",
        ]


class DiscoverLoader(DirectoryLoader):
    """
    This migration group depends on events and transactions
    """

    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.discover")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_discover_merge_table",
            "0002_discover_add_deleted_tags_hash_map",
            "0003_discover_fix_user_column",
            "0004_discover_fix_title_and_message",
            "0005_discover_fix_transaction_name",
            "0006_discover_add_trace_id",
            "0007_discover_add_span_id",
        ]


class OutcomesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.outcomes")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_outcomes",
            "0002_outcomes_remove_size_and_bytes",
            "0003_outcomes_add_category_and_quantity",
            "0004_outcomes_matview_additions",
        ]


class MetricsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.metrics")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_metrics_buckets",
            "0002_metrics_sets",
            "0003_counters_to_buckets",
            "0004_metrics_counters",
            "0005_metrics_distributions_buckets",
            "0006_metrics_distributions",
            "0007_metrics_sets_granularity_10",
            "0008_metrics_counters_granularity_10",
            "0009_metrics_distributions_granularity_10",
            "0010_metrics_sets_granularity_1h",
            "0011_metrics_counters_granularity_1h",
            "0012_metrics_distributions_granularity_1h",
            "0013_metrics_sets_granularity_1d",
            "0014_metrics_counters_granularity_1d",
            "0015_metrics_distributions_granularity_1d",
            "0016_metrics_sets_consolidated_granularity",
            "0017_metrics_counters_consolidated_granularity",
            "0018_metrics_distributions_consolidated_granularity",
            "0019_aggregate_tables_add_ttl",
            "0020_polymorphic_buckets_table",
        ]


class SessionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.sessions")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_sessions", "0002_sessions_aggregates", "0003_sessions_matview"]


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


class ProfilesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.snuba_migrations.profiles")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_profiles"]


_REGISTERED_GROUPS = {
    MigrationGroup.SYSTEM: SystemLoader(),
    MigrationGroup.EVENTS: EventsLoader(),
    MigrationGroup.TRANSACTIONS: TransactionsLoader(),
    MigrationGroup.DISCOVER: DiscoverLoader(),
    MigrationGroup.METRICS: MetricsLoader(),
    MigrationGroup.OUTCOMES: OutcomesLoader(),
    MigrationGroup.SESSIONS: SessionsLoader(),
    MigrationGroup.QUERYLOG: QuerylogLoader(),
    MigrationGroup.SPANS_EXPERIMENTAL: SpansExperimentalLoader(),
    MigrationGroup.PROFILES: ProfilesLoader(),
}


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return _REGISTERED_GROUPS[group]
