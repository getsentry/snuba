from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    """
    Drops the original 'v1' table in favor of the 'v2' table.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SEARCH_ISSUES,
                table_name=params[0],
                target=params[1],
            )
            for params in [
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
                ("search_issues_local", OperationTarget.LOCAL),
            ]
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        # HACK (Volo): This migration is not really reversible. However
        # we have tests that check that all migrations are. Rather than
        # building a way to specify no-reversible migrations,
        # I just copied the CREATE TABLE statement before the DROP so that
        # we could run through all our regular tests. DROPing a table is
        # of course not reversible. Your data will be forever lost
        create_table_statement = """
            CREATE TABLE IF NOT EXISTS search_issues_local
            (
                `organization_id` UInt64,
                `project_id` UInt64,
                `group_id` UInt64,
                `search_title` String,
                `primary_hash` UUID,
                `fingerprint` Array(String),
                `occurrence_id` UUID,
                `occurrence_type_id` UInt16,
                `detection_timestamp` DateTime,
                `event_id` Nullable(UUID),
                `trace_id` Nullable(UUID),
                `platform` LowCardinality(String),
                `environment` LowCardinality(Nullable(String)),
                `release` LowCardinality(Nullable(String)),
                `dist` LowCardinality(Nullable(String)),
                `receive_timestamp` DateTime,
                `client_timestamp` DateTime,
                `tags.key` Array(String),
                `tags.value` Array(String),
                `_tags_hash_map` Array(UInt64) MATERIALIZED arrayMap((k, v) -> cityHash64(concat(replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1'), '=', v)), tags.key, tags.value),
                `user` Nullable(String),
                `user_hash` Nullable(UInt64) MATERIALIZED cityHash64(user),
                `user_id` Nullable(String),
                `user_name` Nullable(String),
                `user_email` Nullable(String),
                `ip_address_v4` Nullable(IPv4),
                `ip_address_v6` Nullable(IPv6),
                `sdk_name` LowCardinality(Nullable(String)),
                `sdk_version` LowCardinality(Nullable(String)),
                `contexts.key` Array(String),
                `contexts.value` Array(String),
                `http_method` LowCardinality(Nullable(String)),
                `http_referer` Nullable(String),
                `deleted` UInt8,
                `message_timestamp` DateTime,
                `partition` UInt16,
                `offset` UInt64,
                `retention_days` UInt16
            )
            ENGINE = ReplacingMergeTree(deleted)
            PARTITION BY (retention_days, toMonday(receive_timestamp))
            ORDER BY (project_id, toStartOfDay(receive_timestamp), primary_hash, cityHash64(occurrence_id))
            SAMPLE BY cityHash64(occurrence_id)
            TTL receive_timestamp + toIntervalDay(retention_days)
            SETTINGS index_granularity = 8192
        """
        return [
            operations.RunSql(
                StorageSetKey.SEARCH_ISSUES,
                create_table_statement.format(table_name),
                target,
            )
            for table_name, target in [
                ("search_issues_local", OperationTarget.LOCAL),
                ("search_issues_dist", OperationTarget.DISTRIBUTED),
            ]
        ]
