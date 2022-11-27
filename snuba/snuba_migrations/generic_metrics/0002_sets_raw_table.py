from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    Float,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False
    local_table_name = "generic_metric_sets_raw_local"
    dist_table_name = "generic_metric_sets_raw_dist"
    columns: Sequence[Column[Modifiers]] = [
        Column("use_case_id", String(Modifiers(low_cardinality=True))),
        Column("org_id", UInt(64)),
        Column("project_id", UInt(64)),
        Column("metric_id", UInt(64)),
        Column("timestamp", DateTime()),
        Column("retention_days", UInt(16)),
        Column(
            "tags",
            Nested(
                [
                    ("key", UInt(64)),
                    ("indexed_value", UInt(64)),
                    ("raw_value", String()),
                ]
            ),
        ),
        Column("set_values", Array(UInt(64))),
        Column("count_value", Float(64)),
        Column("distribution_values", Array(Float(64))),
        Column("metric_type", String(Modifiers(low_cardinality=True))),
        Column("materialization_version", UInt(8)),
        Column("timeseries_id", UInt(32)),
        Column("partition", UInt(16)),
        Column("offset", UInt(64)),
    ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.local_table_name,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                    order_by="(use_case_id, org_id, project_id, metric_id, timestamp)",
                    partition_by="(toStartOfInterval(timestamp, toIntervalDay(3)))",
                    ttl="timestamp + toIntervalDay(7)",
                ),
                columns=self.columns,
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.local_table_name,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.dist_table_name,
                engine=table_engines.Distributed(
                    local_table_name=self.local_table_name,
                    sharding_key="cityHash64(timeseries_id)",
                ),
                columns=self.columns,
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.dist_table_name,
            )
        ]
