from __future__ import annotations

from abc import ABC, abstractmethod
from glob import glob
from importlib import import_module
from typing import Sequence

from snuba import settings
from snuba.datasets.configuration.migration_parser import load_migration_group
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


class ConfigurationLoader(DirectoryLoader):
    """
    Loads migration groups from YAML configuration files.
    """

    def __init__(
        self, path_to_migration_group_config: str, migration_path: str | None = None
    ) -> None:
        self.migration_group = load_migration_group(path_to_migration_group_config)
        self.migration_group_name = self.migration_group["name"]
        self.migration_names = [
            str(migration) for migration in self.migration_group["migrations"]
        ]
        self.optional = self.migration_group.get("optional", False)
        migration_path = migration_path or "snuba.snuba_migrations"
        super().__init__(f"{migration_path}.{self.migration_group_name}")

    def get_migrations(self) -> Sequence[str]:
        return self.migration_names


class SystemLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.migrations.system_migrations")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_migrations"]


class EventsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.events")

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
            "0016_drop_legacy_events",
        ]


class TransactionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.transactions")

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
            "0014_transactions_remove_flattened_columns",
            "0015_transactions_add_source_column",
            "0016_transactions_add_group_ids_column",
            "0017_transactions_add_app_start_type_column",
        ]


class DiscoverLoader(DirectoryLoader):
    """
    This migration group depends on events and transactions
    """

    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.discover")

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
        super().__init__("snuba.snuba_migrations.outcomes")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_outcomes",
            "0002_outcomes_remove_size_and_bytes",
            "0003_outcomes_add_category_and_quantity",
            "0004_outcomes_matview_additions",
        ]


class ReplaysLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.replays")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_replays",
            "0002_add_url",
            "0003_alter_url_allow_null",
            "0004_add_error_ids_column",
            "0005_add_urls_user_agent_replay_start_timestamp",
            "0006_add_is_archived_column",
        ]


class MetricsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.metrics")

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
            "0021_polymorphic_bucket_materialized_views",
            "0022_repartition_polymorphic_table",
            "0023_polymorphic_repartitioned_bucket_matview",
            "0024_metrics_distributions_add_histogram",
            "0025_metrics_counters_aggregate_v2",
            "0026_metrics_counters_v2_writing_matview",
            "0027_fix_migration_0026",
            "0028_metrics_sets_aggregate_v2",
            "0029_metrics_distributions_aggregate_v2",
            "0030_metrics_distributions_v2_writing_mv",
            "0031_metrics_sets_v2_writing_mv",
            "0032_redo_0030_and_0031_without_timestamps",
            "0033_metrics_cleanup_old_views",
            "0034_metrics_cleanup_old_tables",
        ]


class SessionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.sessions")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_sessions",
            "0002_sessions_aggregates",
            "0003_sessions_matview",
            "0004_sessions_ttl",
        ]


class QuerylogLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.querylog")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_querylog", "0002_status_type_change", "0003_add_profile_fields"]


class ProfilesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.profiles")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_profiles",
            "0002_disable_vertical_merge_algorithm",
            "0003_add_device_architecture",
        ]


class FunctionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.functions")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_functions"]


MigrationGroup = str

CONFIG_BUILT_MIGRATIONS = {
    MigrationGroup(loader.migration_group_name): loader
    for loader in [
        ConfigurationLoader(config_file)
        for config_file in glob(settings.MIGRATION_CONFIG_FILES_GLOB, recursive=True)
    ]
}


# This is a list instead of a dictionary to preserve the order
# of existing migrations. For example, the discover migrations
# need to run after the events/transactions migrations.
REGISTERED_GROUPS: list[tuple[str, GroupLoader]] = [
    ("system", SystemLoader()),
    ("events", EventsLoader()),
    ("transactions", TransactionsLoader()),
    ("discover", DiscoverLoader()),
    ("metrics", MetricsLoader()),
    ("outcomes", OutcomesLoader()),
    ("sessions", SessionsLoader()),
    ("querylog", QuerylogLoader()),
    ("profiles", ProfilesLoader()),
    ("functions", FunctionsLoader()),
    ("replays", ReplaysLoader()),
]

# TODO: config defined migrations are not run in any particular order.
# In the future we will need the ability to ensure migrations run in
# the right order.
REGISTERED_GROUPS.extend(
    [(name, loader) for name, loader in CONFIG_BUILT_MIGRATIONS.items()]
)

REGISTERED_GROUPS_LOOKUP = {k: v for (k, v) in REGISTERED_GROUPS}

# Migration groups are mandatory by default, unless they are on this list
OPTIONAL_GROUPS = {
    "metrics",
    "sessions",
    "querylog",
    "profiles",
    "functions",
    "replays",
}

OPTIONAL_GROUPS.update(
    set([m for m in CONFIG_BUILT_MIGRATIONS if CONFIG_BUILT_MIGRATIONS[m].optional])
)


def get_group_loader(group: MigrationGroup) -> GroupLoader:
    return REGISTERED_GROUPS_LOOKUP[group]
