from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines


columns = [
    Column("timestamp", DateTime()),
    Column("name", String()),
    Column("tags", Nested([Column("key", String()), Column("value", String())])),
    Column("tags_hash", Array(UInt(64))),
    Column("count", UInt(64)),
    Column("quantiles_sketch", Array(UInt(8))),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.METRICS,
                    order_by="(toStartOfDay(timestamp), cityHash64(name))",
                    partition_by="(toMonday(timestamp))",
                    sample_by="cityHash64(name)",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="metrics_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_dist",
            )
        ]
