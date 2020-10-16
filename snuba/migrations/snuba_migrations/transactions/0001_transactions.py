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
    Column("transaction_name", LowCardinality(String())),
    Column("transaction_hash", Materialized(UInt(64), "cityHash64(transaction_name)")),
    Column("transaction_op", LowCardinality(String())),
    Column("transaction_status", WithDefault(UInt(8), str(UNKNOWN_SPAN_STATUS))),
    Column("start_ts", DateTime()),
    Column("start_ms", UInt(16)),
    Column("finish_ts", DateTime()),
    Column("finish_ms", UInt(16)),
    Column("duration", UInt(32)),
    Column("platform", LowCardinality(String())),
    Column("environment", LowCardinality(Nullable(String()))),
    Column("release", LowCardinality(Nullable(String()))),
    Column("dist", LowCardinality(Nullable(String()))),
    Column("ip_address_v4", Nullable(IPv4())),
    Column("ip_address_v6", Nullable(IPv6())),
    Column("user", WithDefault(String(), "''",)),
    Column("user_hash", Materialized(UInt(64), "cityHash64(user)")),
    Column("user_id", Nullable(String())),
    Column("user_name", Nullable(String())),
    Column("user_email", Nullable(String())),
    Column("sdk_name", WithDefault(LowCardinality(String()), "''")),
    Column("sdk_version", WithDefault(LowCardinality(String()), "''")),
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
