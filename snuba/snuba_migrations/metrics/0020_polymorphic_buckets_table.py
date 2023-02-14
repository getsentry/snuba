from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.utils.schemas import Array, Column, DateTime, Float, Nested, String, UInt


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Create a polymorphic bucket table for metrics_sets,
    metrics_distributions, and metrics_counters
    """

    blocking = False
    local_table_name = "metrics_raw_local"
    dist_table_name = "metrics_raw_dist"

    column_list: Sequence[Column[Modifiers]] = [
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("timestamp", DateTime()),
        Column(
            "tags",
            Nested([Column("key", UInt(64)), Column("value", UInt(64))]),
        ),
        Column("metric_type", String(Modifiers(low_cardinality=True))),
        Column("set_values", Array(UInt(64))),
        Column("count_value", Float(64)),
        Column("distribution_values", Array(Float(64))),
        Column("materialization_version", UInt(8)),
        Column("retention_days", UInt(16)),
        Column("partition", UInt(16)),
        Column("offset", UInt(64)),
    ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name=self.local_table_name,
                columns=self.column_list,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.METRICS,
                    order_by="(use_case_id, metric_type, org_id, project_id, metric_id, timestamp)",
                    partition_by="(toStartOfDay(timestamp))",
                    ttl="timestamp + toIntervalDay(7)",
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name=self.local_table_name
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.METRICS,
                table_name=self.dist_table_name,
                columns=self.column_list,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name,
                    sharding_key=None,
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.METRICS, table_name=self.dist_table_name
            )
        ]
