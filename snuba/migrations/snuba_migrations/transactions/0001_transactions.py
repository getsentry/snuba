from copy import deepcopy
from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import LowCardinality, Materialized, WithDefault

UNKNOWN_SPAN_STATUS = 2

columns = [
    Column("project_id", UInt(64)),
    Column("event_id", UUID()),
    Column("trace_id", UUID()),
    Column("span_id", UInt(64)),
    Column("transaction_name", String([LowCardinality()])),
    Column(
        "transaction_hash", UInt(64, [Materialized("cityHash64(transaction_name)")])
    ),
    Column("transaction_op", String([LowCardinality()])),
    Column("transaction_status", UInt(8, [WithDefault(str(UNKNOWN_SPAN_STATUS))])),
    Column("start_ts", DateTime()),
    Column("start_ms", UInt(16)),
    Column("finish_ts", DateTime()),
    Column("finish_ms", UInt(16)),
    Column("duration", UInt(32)),
    Column("platform", String([LowCardinality()])),
    Column("environment", String([Nullable(), LowCardinality()])),
    Column("release", String([Nullable(), LowCardinality()])),
    Column("dist", String([Nullable(), LowCardinality()])),
    Column("ip_address_v4", IPv4([Nullable()])),
    Column("ip_address_v6", IPv6([Nullable()])),
    Column("user", String([WithDefault("''")])),
    Column("user_hash", UInt(64, [Materialized("cityHash64(user)")])),
    Column("user_id", String([Nullable()])),
    Column("user_name", String([Nullable()])),
    Column("user_email", String([Nullable()])),
    Column("sdk_name", String([LowCardinality(), WithDefault("''")])),
    Column("sdk_version", String([LowCardinality(), WithDefault("''")])),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("_contexts_flattened", String()),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.TRANSACTIONS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(span_id))",
                    partition_by="(retention_days, toMonday(finish_ts))",
                    sample_by="cityHash64(span_id)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="transactions_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        # We removed the materialized for the dist table DDL.
        def strip_materialized(columns: Sequence[Column]) -> None:
            for col in columns:
                if isinstance(col.type, Materialized):
                    col.type = col.type.inner_type

        dist_columns = deepcopy(columns)
        strip_materialized(dist_columns)

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                columns=dist_columns,
                engine=table_engines.Distributed(
                    local_table_name="transactions_local",
                    sharding_key="cityHash64(span_id)",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="transactions_dist",
            )
        ]
