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


class Migration(migration.MultiStepMigration):
    # TODO: Provide the forwards_dist and backwards_dist methods for this migration
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
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

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_local",
                columns=columns,
                engine=table_engines.MergeTree(
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
