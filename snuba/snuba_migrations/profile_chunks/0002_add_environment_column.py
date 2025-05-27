from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    storage_set = StorageSetKey.PROFILE_CHUNKS
    table_prefix = "profile_chunks"
    local_table_name = f"{table_prefix}_local"
    dist_table_name = f"{table_prefix}_dist"

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column=Column(
                    "environment",
                    String(Modifiers(nullable=True, low_cardinality=True)),
                ),
                after="end_timestamp",
                target=target,
            )
            for table_name, target in [
                (self.local_table_name, OperationTarget.LOCAL),
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=self.storage_set,
                table_name=table_name,
                column_name="environment",
                target=target,
            )
            for table_name, target in [
                (self.dist_table_name, OperationTarget.DISTRIBUTED),
                (self.local_table_name, OperationTarget.LOCAL),
            ]
        ]
