from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    Column,
    DateTime,
    Enum,
    Float,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

status_type = Enum[Modifiers]([("success", 0), ("error", 1), ("rate-limited", 2)])

columns: Sequence[Column[Modifiers]] = [
    Column("request_id", UUID()),
    Column("request_body", String()),
    Column("referrer", String(Modifiers(low_cardinality=True))),
    Column("dataset", String(Modifiers(low_cardinality=True))),
    Column("projects", Array(UInt(64))),
    Column("organization", UInt(64, Modifiers(nullable=True))),
    Column("timestamp", DateTime()),
    Column("duration_ms", UInt(32)),
    Column("status", status_type),
    Column(
        "clickhouse_queries",
        Nested(
            [
                Column("sql", String()),
                Column("status", status_type),
                Column("trace_id", UUID(Modifiers(nullable=True))),
                Column("duration_ms", UInt(32)),
                Column("stats", String()),
                Column("final", UInt(8)),
                Column("cache_hit", UInt(8)),
                Column("sample", Float(32)),
                Column("max_threads", UInt(8)),
                Column("num_days", UInt(32)),
                Column("clickhouse_table", String(Modifiers(low_cardinality=True))),
                Column("query_id", String()),
                Column("is_duplicate", UInt(8)),
                Column("consistent", UInt(8)),
            ]
        ),
    ),
]


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.QUERYLOG,
                    order_by="(toStartOfDay(timestamp), request_id)",
                    partition_by="(toMonday(timestamp))",
                    # sample_by="request_id",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="querylog_local",
                    sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="querylog_dist",
            )
        ]
