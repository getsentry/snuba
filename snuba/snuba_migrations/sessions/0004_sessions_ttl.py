from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
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
        """
        Removing a TTL is not supported by Clickhouse version 20.3. So there is
        no backwards migration.
        """
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
