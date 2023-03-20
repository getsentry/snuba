from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
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
                column=Column("replay_id", UUID(Modifiers(nullable=True))),
                after="modules.version",
                target=OperationTarget.DISTRIBUTED,
            )
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name=table_name,
                column_name="replay_id",
                target=OperationTarget.DISTRIBUTED,
            )
        ]
