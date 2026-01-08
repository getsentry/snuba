from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the timeseries_id column to the metrics raw table so we can add
    a sharding key for each timeseries and scale the release health cluster.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=table_name,
                column=Column("timeseries_id", UInt(32)),
                target=target,
            )
            for table_name, target in [
                ("metrics_raw_v2_local", OperationTarget.LOCAL),
                ("metrics_raw_v2_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.METRICS,
                table_name=table_name,
                column_name="timeseries_id",
                target=target,
            )
            for table_name, target in [
                ("metrics_raw_v2_dist", OperationTarget.DISTRIBUTED),
                ("metrics_raw_v2_local", OperationTarget.LOCAL),
            ]
        ]
