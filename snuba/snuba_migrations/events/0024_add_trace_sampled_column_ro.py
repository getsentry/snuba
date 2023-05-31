from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

table_name = "errors_dist_ro"


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name=table_name,
                column=Column("trace_sampled", UInt(8, Modifiers(nullable=True))),
                after="exception_main_thread",
                target=OperationTarget.DISTRIBUTED,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name=table_name,
                column_name="exception_main_thread",
                target=OperationTarget.DISTRIBUTED,
            )
        ]
