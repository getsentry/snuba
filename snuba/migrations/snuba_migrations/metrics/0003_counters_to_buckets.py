from typing import Sequence

from snuba.clickhouse.columns import Column, Float
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_buckets_local",
                column=Column("counter_value", Float(64)),
                after="set_values",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.METRICS, "metrics_buckets_local", "counter_value"
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name="metrics_buckets_dist",
                column=Column("counter_value", Float(64)),
                after="set_values",
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.METRICS, "metrics_buckets_dist", "counter_value"
            )
        ]
