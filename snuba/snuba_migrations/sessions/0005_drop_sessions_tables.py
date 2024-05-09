from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_dist",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_dist",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_mv_local",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_hourly_local",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropTable(
                storage_set=StorageSetKey.SESSIONS,
                table_name="sessions_raw_local",
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        # Sessions is fully deprecated, we don't need the reverse migration to bring back the tables
        return []
