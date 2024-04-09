from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    local_table_name = "generic_metric_{table}_raw_local"
    dist_table_name = "generic_metric_{table}_raw_dist"
    storage_set_keys = {
        "sets": StorageSetKey.GENERIC_METRICS_SETS,
        "distributions": StorageSetKey.GENERIC_METRICS_DISTRIBUTIONS,
        "gauges": StorageSetKey.GENERIC_METRICS_GAUGES,
    }

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = []
        for table in ["sets", "distributions", "gauges"]:
            ops.append(
                operations.AddColumn(
                    storage_set=self.storage_set_keys[table],
                    table_name=self.local_table_name.format(table=table),
                    column=Column("record_meta", UInt(8, Modifiers(default=str("0")))),
                    target=operations.OperationTarget.LOCAL,
                    after="materialization_version",
                )
            )
            ops.append(
                operations.AddColumn(
                    storage_set=self.storage_set_keys[table],
                    table_name=self.dist_table_name.format(table=table),
                    column=Column("record_meta", UInt(8, Modifiers(default=str("0")))),
                    target=operations.OperationTarget.DISTRIBUTED,
                    after="materialization_version",
                )
            )

        return ops

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        ops = []
        for table in ["sets", "distributions", "gauges"]:
            ops.append(
                operations.DropColumn(
                    column_name="record_meta",
                    storage_set=self.storage_set_keys[table],
                    table_name=self.dist_table_name.format(table=table),
                    target=operations.OperationTarget.DISTRIBUTED,
                )
            )
            ops.append(
                operations.DropColumn(
                    column_name="record_meta",
                    storage_set=self.storage_set_keys[table],
                    table_name=self.local_table_name.format(table=table),
                    target=operations.OperationTarget.LOCAL,
                )
            )

        return ops
