from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    local_table_name = "generic_metric_gauges_raw_local"
    dist_table_name = "generic_metric_gauges_raw_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_GAUGES

    columns: Sequence[Column[MigrationModifiers]] = [
        Column(
            "sampling_weight",
            UInt(64, MigrationModifiers(codecs=["ZSTD(1)"], default=str("1"))),
        ),
    ]

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column=column,
                target=target,
            )
            for column in self.columns
            for table_name, target in [
                (self.local_table_name, OperationTarget.LOCAL),
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=self.storage_set_key,
                table_name=table_name,
                column_name=column.name,
                target=target,
            )
            for column in self.columns
            for table_name, target in [
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
                (self.local_table_name, OperationTarget.LOCAL),
            ]
        ]
