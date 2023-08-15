from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds group_raw column to store the hash of the raw description.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "nodestore_version",
                    UInt(8, modifiers=MigrationModifiers(codecs=["Delta"])),
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "nodestore_version",
                    UInt(8, modifiers=MigrationModifiers(codecs=["Delta"])),
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="nodestore_version",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="nodestore_version",
                target=OperationTarget.LOCAL,
            ),
        ]
