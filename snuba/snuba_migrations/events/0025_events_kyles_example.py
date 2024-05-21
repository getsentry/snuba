from typing import Sequence

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import UInt


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column=Column("has_purple_cat", UInt(8)),
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
                column_name="has_purple_cat",
                target=target,
            )
            for table_name, target in [
                ("errors_dist", OperationTarget.DISTRIBUTED),
                ("errors_local", OperationTarget.LOCAL),
            ]
        ]
