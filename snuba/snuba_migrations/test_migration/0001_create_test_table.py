from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [Column("test_col", UInt(8))]


class Migration(migration.ClickhouseNodeMigration):
    """
    This is a test migration. It creates a test tables.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:

        return [
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_local",
                target=operations.OperationTarget.LOCAL,
                columns=columns,
                engine=table_engines.MergeTree(
                    storage_set=StorageSetKey.QUERYLOG,
                    order_by="test_col",
                    partition_by="test_col",
                    sample_by="test_col",
                ),
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_dist",
                target=operations.OperationTarget.DISTRIBUTED,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="test_migration_local",
                    sharding_key=None,
                ),
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_dist",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.QUERYLOG,
                table_name="test_migration_local",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
