from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers

table_name_prefix = "spans"
column_name = "module"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_local",
                column_name=column_name,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.SPANS,
                table_name=f"{table_name_prefix}_local",
                column=Column(
                    column_name,
                    String(MigrationModifiers(low_cardinality=True, nullable=True)),
                ),
                target=operations.OperationTarget.LOCAL,
            ),
        ]
