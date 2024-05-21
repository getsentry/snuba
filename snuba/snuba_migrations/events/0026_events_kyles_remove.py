from typing import Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import String
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column_name="level",
                target=target,
            )
            for table_name, target in [
                ("errors_dist", OperationTarget.DISTRIBUTED),
                ("errors_local", OperationTarget.LOCAL),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column=Column("level", String(Modifiers(nullable=True))),
                after="culprit",
                target=target,
            )
            for table_name, target in [
                ("errors_local", OperationTarget.LOCAL),
                ("errors_dist", OperationTarget.DISTRIBUTED),
            ]
        ]
