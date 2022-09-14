from typing import Sequence

from snuba.clusters.storage_set_key import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers
from snuba.utils.schemas import Array, Column, UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    table_name = "generic_metric_sets_raw_local"
    new_column: Column[MigrationModifiers] = Column("granularities", Array(UInt(8)))

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.table_name,
                column=self.new_column,
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.table_name,
                column_name=self.new_column.name,
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.table_name,
                column=self.new_column,
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.GENERIC_METRICS_SETS,
                table_name=self.table_name,
                column_name=self.new_column.name,
            )
        ]
