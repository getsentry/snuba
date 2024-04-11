from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    local_table_name = "generic_metric_sets_raw_local"
    dist_table_name = "generic_metric_sets_raw_dist"
    storage_set_key = StorageSetKey.GENERIC_METRICS_SETS

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = [
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.local_table_name,
                column=Column("record_meta", UInt(8, Modifiers(default=str("0")))),
                target=operations.OperationTarget.LOCAL,
                after="materialization_version",
            ),
            operations.AddColumn(
                storage_set=self.storage_set_key,
                table_name=self.dist_table_name,
                column=Column("record_meta", UInt(8, Modifiers(default=str("0")))),
                target=operations.OperationTarget.DISTRIBUTED,
                after="materialization_version",
            ),
        ]

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = []
        for table in ["sets", "distributions", "gauges"]:
            ops.append(
                operations.DropColumn(
                    column_name="record_meta",
                    storage_set=self.storage_set_key,
                    table_name=self.dist_table_name.format(table=table),
                    target=operations.OperationTarget.DISTRIBUTED,
                )
            )
            ops.append(
                operations.DropColumn(
                    column_name="record_meta",
                    storage_set=self.storage_set_key,
                    table_name=self.local_table_name.format(table=table),
                    target=operations.OperationTarget.LOCAL,
                )
            )

        return ops
