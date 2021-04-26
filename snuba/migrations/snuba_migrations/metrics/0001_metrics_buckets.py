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
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("metric_id", UInt(64)),
    Column("metric_type", String(Modifiers(low_cardinality=True))),
    Column("timestamp", DateTime()),
    Column("tags", Nested([Column("key", UInt(64)), Column("value", UInt(64))])),
    Column("set_values", Array(UInt(64))),
    Column("materialization_version", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_buckets_local",
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.METRICS,
                    order_by="(metric_type, org_id, project_id, metric_id, timestamp)",
                    partition_by="toMonday(timestamp)",
                    ttl="timestamp + toIntervalDay(14)",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_buckets_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_buckets_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="metrics_buckets_local", sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name="metrics_buckets_dist",
            )
        ]
