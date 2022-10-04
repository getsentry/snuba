from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyTableTTL(
                StorageSetKey.SESSIONS, "sessions_raw_local", "started", 30
            ),
            operations.ModifyTableTTL(
                StorageSetKey.SESSIONS, "sessions_hourly_local", "started", 90
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.RemoveTableTTL(StorageSetKey.SESSIONS, "sessions_raw_local"),
            operations.RemoveTableTTL(StorageSetKey.SESSIONS, "sessions_hourly_local"),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
