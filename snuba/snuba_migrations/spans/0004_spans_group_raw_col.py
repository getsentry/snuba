from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds group_raw columns to store the scrubbed group hash.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "group_raw",
                    UInt(64),
                ),
                target=OperationTarget.LOCAL,
                after="group",
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "group_raw",
                    UInt(64),
                ),
                target=OperationTarget.DISTRIBUTED,
                after="group",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="group_raw",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="group_raw",
                target=OperationTarget.LOCAL,
            ),
        ]
