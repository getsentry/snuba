from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    """
    Add replay_id to merge table
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("replay_id", UUID(Modifiers(nullable=True))),
                after="span_id",
                target=target,
            )
            for table_name, target in [
                ("discover_local", OperationTarget.LOCAL),
                ("discover_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column_name="replay_id",
                target=target,
            )
            for table_name, target in [
                ("discover_dist", OperationTarget.DISTRIBUTED),
                ("discover_local", OperationTarget.LOCAL),
            ]
        ]
