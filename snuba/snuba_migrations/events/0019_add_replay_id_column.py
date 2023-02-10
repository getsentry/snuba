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
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column=Column("replay_id", UUID(Modifiers(nullable=True))),
                after="modules.version",
                target=target,
            )
            for table_name, target in [
                ("errors_local", OperationTarget.LOCAL),
                ("errors_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column_name="replay_id",
                target=target,
            )
            for table_name, target in [
                ("errors_dist", OperationTarget.DISTRIBUTED),
                ("errors_local", OperationTarget.LOCAL),
            ]
        ]
