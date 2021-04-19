from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Truncate the events table. Cannot be reversed.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.TruncateTable(
                storage_set=StorageSetKey.EVENTS, table_name="sentry_local"
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
