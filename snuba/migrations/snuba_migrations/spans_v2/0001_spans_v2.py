from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, Float, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("transaction_id", UUID()),
    Column("trace_id", UUID()),
    Column("transaction_span_id", UInt(64)),
    Column("span_id", UInt(64)),
    Column("parent_span_id", UInt(64, Modifiers(nullable=True))),
    Column("transaction_name", String()),
    Column("span_group_id", UUID()),
    Column("op", String()),
    Column("status", UInt(8)),
    # transaction start/finish times
    Column("start_ts", DateTime()),
    Column("start_ns", UInt(32)),
    Column("finish_ts", DateTime()),
    Column("finish_ns", UInt(32)),
    # span start/finish times
    Column("span_start_ts", DateTime()),
    Column("span_start_ns", UInt(32)),
    Column("span_finish_ts", DateTime()),
    Column("span_finish_ns", UInt(32)),
    Column("duration_ms", UInt(32)),
    Column("exclusive_time_ms", Float(64)),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_v2_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.TRANSACTIONS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(finish_ts), transaction_name, cityHash64(transaction_span_id), cityHash64(span_id))",
                    partition_by="(toMonday(finish_ts))",
                    sample_by="cityHash64(transaction_span_id)",
                    ttl="finish_ts + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="spans_v2_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="spans_v2_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="spans_experimental_local",
                    sharding_key="cityHash64(transaction_span_id)",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.TRANSACTIONS, table_name="spans_v2_dist",
            )
        ]
