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
    Adds columns start_ms and end_ms to the spans table. These columns would contain the
    millisecond precision of start_timestamp and end_timestamp respectively.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "start_ms",
                    UInt(16),
                ),
                target=OperationTarget.LOCAL,
                after="start_timestamp",
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "end_ms",
                    UInt(16),
                ),
                target=OperationTarget.LOCAL,
                after="end_timestamp",
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "start_ms",
                    UInt(16),
                ),
                target=OperationTarget.DISTRIBUTED,
                after="start_timestamp",
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "end_ms",
                    UInt(16),
                ),
                target=OperationTarget.DISTRIBUTED,
                after="end_timestamp",
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="end_ms",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="start_ms",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="end_ms",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="start_ms",
                target=OperationTarget.LOCAL,
            ),
        ]
