from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [Column("test_col", UInt(8))]


class Migration(migration.ClickhouseNodeMigration):
    """
    This is a test migration. It add a column to the test tables.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:

        return [
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_local",
                target=operations.OperationTarget.LOCAL,
                column=Column("test_col_1", UInt(8)),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_dist",
                target=operations.OperationTarget.DISTRIBUTED,
                column=Column("test_col_1", UInt(8)),
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_local",
                target=operations.OperationTarget.LOCAL,
                column_name="test_col_1",
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_dist",
                target=operations.OperationTarget.DISTRIBUTED,
                column_name="test_col_1",
            ),
        ]
