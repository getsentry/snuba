from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_local",
                column=Column(
                    "group_first_release_id", UInt(64, Modifiers(nullable=True))
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_dist",
                column=Column(
                    "group_first_release_id", UInt(64, Modifiers(nullable=True))
                ),
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_local",
                column=Column("group_first_release_id", UUID(Modifiers(nullable=True))),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.GROUP_ATTRIBUTES,
                table_name="group_attributes_dist",
                column=Column("group_first_release_id", UUID(Modifiers(nullable=True))),
            ),
        ]
