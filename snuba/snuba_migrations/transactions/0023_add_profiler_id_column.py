from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column("profiler_id", UUID(Modifiers(nullable=True))),
                after="profile_id",
                target=target,
            )
            for table_name, target in [
                ("transactions_local", OperationTarget.LOCAL),
                ("transactions_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column_name="profiler_id",
                target=target,
            )
            for table_name, target in [
                ("transactions_dist", OperationTarget.DISTRIBUTED),
                ("transactions_local", OperationTarget.LOCAL),
            ]
        ]
