from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_local",
                column=Column("group_priority", UInt(8, Modifiers(nullable=True))),
                target=OperationTarget.LOCAL,
                after="group_substatus",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_dist",
                column=Column("group_priority", UInt(8, Modifiers(nullable=True))),
                target=OperationTarget.DISTRIBUTED,
                after="group_substatus",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_dist",
                column_name="group_priority",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_local",
                column_name="group_priority",
                target=OperationTarget.LOCAL,
            ),
        ]
