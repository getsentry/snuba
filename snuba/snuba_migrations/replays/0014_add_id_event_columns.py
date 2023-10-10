from typing import Sequence

from snuba.clickhouse.columns import UUID, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget

LOG_LEVELS = ["fatal", "error", "warning", "info", "debug"]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name=table_name,
                column=Column(log_level + "_id", UUID()),
                after="replay_id",
                target=target,
            )
            for log_level in LOG_LEVELS
            for table_name, target in [
                ("replays_local", OperationTarget.LOCAL),
                ("replays_dist", OperationTarget.DISTRIBUTED),
            ]
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name=table_name,
                column_name=log_level + "_id",
                target=target,
            )
            for log_level in LOG_LEVELS
            for table_name, target in [
                ("replays_dist", OperationTarget.DISTRIBUTED),
                ("replays_local", OperationTarget.LOCAL),
            ]
        ]
