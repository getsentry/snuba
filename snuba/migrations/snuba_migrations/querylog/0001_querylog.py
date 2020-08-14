from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Enum,
    Float,
    LowCardinality,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines


status_type = Enum([("success", 0), ("error", 1), ("rate-limited", 2)])

columns = [
    Column("request_id", UUID()),
    Column("request_body", String()),
    Column("referrer", LowCardinality(String())),
    Column("dataset", LowCardinality(String())),
    Column("projects", Array(UInt(64))),
    Column("organization", Nullable(UInt(64))),
    Column("timestamp", DateTime()),
    Column("duration_ms", UInt(32)),
    Column("status", status_type),
    Column(
        "clickhouse_queries",
        Nested(
            [
                Column("sql", String()),
                Column("status", status_type),
                Column("trace_id", Nullable(UUID())),
                Column("duration_ms", UInt(32)),
                Column("stats", String()),
                Column("final", UInt(8)),
                Column("cache_hit", UInt(8)),
                Column("sample", Float(32)),
                Column("max_threads", UInt(8)),
                Column("num_days", UInt(32)),
                Column("clickhouse_table", LowCardinality(String())),
                Column("query_id", String()),
                Column("is_duplicate", UInt(8)),
                Column("consistent", UInt(8)),
            ]
        ),
    ),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.QUERYLOG,
                    order_by="(toStartOfDay(timestamp), request_id)",
                    partition_by="(toMonday(timestamp))",
                    sample_by="request_id",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG, table_name="querylog_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="querylog_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG, table_name="querylog_dist",
            )
        ]
