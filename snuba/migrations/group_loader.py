from __future__ import annotations

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
    """

    def __init__(self, module_path: str) -> None:
        self.__module = module_path

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
            "0017_errors_add_indexes",
            "0018_errors_ro_add_tags_hash_map",
            "0019_add_replay_id_column",
            "0020_add_main_thread_column",
            "0021_add_replay_id_errors_ro",
            "0022_add_main_thread_column_errors_ro",
            "0023_add_trace_sampled_num_processing_errors_columns",
            "0024_add_trace_sampled_num_processing_errors_columns_ro",
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
            "0018_transactions_add_profile_id",
            "0019_transactions_add_indexes_and_context_hash",
            "0020_transactions_add_codecs",
            "0021_transactions_add_replay_id",
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
            "0005_outcomes_ttl",
            "0006_outcomes_add_size_col",
            "0007_outcomes_add_event_id_ttl_codec",
            "0008_outcomes_add_indexes",
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
            "0007_add_replay_type_column",
            "0008_add_sample_rate",
            "0009_add_dom_index_columns",
            "0010_add_nullable_columns",
            "0011_add_is_dead_rage",
            "0012_materialize_counts",
            "0013_add_low_cardinality_codecs",
            "0014_add_id_event_columns",
            "0015_index_frequently_accessed_columns",
            "0016_materialize_new_event_counts",
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
            "0035_metrics_raw_timeseries_id",
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
        return [
            "0001_querylog",
            "0002_status_type_change",
            "0003_add_profile_fields",
            "0004_add_bytes_scanned",
            "0005_add_codec_update_settings",
            "0006_sorting_key_change",
        ]


class TestMigrationLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.test_migration")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_create_test_table", "0002_add_test_col"]


class ProfilesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.profiles")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_profiles",
            "0002_disable_vertical_merge_algorithm",
            "0003_add_device_architecture",
            "0004_drop_profile_column",
        ]


class FunctionsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.functions")

    def get_migrations(self) -> Sequence[str]:
        return ["0001_functions", "0002_add_new_columns_to_raw_functions"]


class GenericMetricsLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.generic_metrics")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_sets_aggregate_table",
            "0002_sets_raw_table",
            "0003_sets_mv",
            "0004_sets_raw_add_granularities",
            "0005_sets_replace_mv",
            "0006_sets_raw_add_granularities_dist_table",
            "0007_distributions_aggregate_table",
            "0008_distributions_raw_table",
            "0009_distributions_mv",
            "0010_counters_aggregate_table",
            "0011_counters_raw_table",
            "0012_counters_mv",
            "0013_distributions_dist_tags_hash",
            "0014_distribution_add_options",
            "0015_sets_add_options",
            "0016_counters_add_options",
            "0017_distributions_mv2",
            "0018_sets_update_opt_default",
            "0019_counters_update_opt_default",
            "0020_sets_mv2",
            "0021_counters_mv2",
            "0022_gauges_aggregate_table",
            "0023_gauges_raw_table",
            "0024_gauges_mv",
            "0025_counters_add_raw_tags_hash_column",
            "0026_gauges_add_raw_tags_hash_column",
            "0027_sets_add_raw_tags_column",
            "0028_distributions_add_indexed_tags_column",
        ]


class SearchIssuesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.search_issues")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_search_issues",
            "0002_search_issues_add_tags_hash_map",
            "0003_search_issues_modify_occurrence_type_id_size",
            "0004_rebuild_search_issues_with_version",
            "0005_search_issues_v2",
            "0006_add_subtitle_culprit_level_resource_id",
            "0007_add_transaction_duration",
            "0008_add_profile_id_replay_id",
            "0009_add_message",
        ]


class SpansLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.spans")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_spans_v1",
            "0002_spans_add_tags_hashmap",
            "0003_spans_add_ms_columns",
            "0004_spans_group_raw_col",
            "0005_spans_add_sentry_tags",
            "0006_spans_add_profile_id",
            "0007_spans_add_metrics_summary",
        ]


class GroupAttributesLoader(DirectoryLoader):
    def __init__(self) -> None:
        super().__init__("snuba.snuba_migrations.group_attributes")

    def get_migrations(self) -> Sequence[str]:
        return [
            "0001_group_attributes",
        ]
