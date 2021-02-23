from typing import Sequence
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    We added the size and bytes_received columns on 15 Dec 2019 and reverted the next
    day. This migration ensures the column is dropped for all users on the off chance
    they had upgraded or attempted to upgrade to a version released in that window.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.OUTCOMES, "outcomes_raw_local", "size"),
            operations.DropColumn(
                StorageSetKey.OUTCOMES, "outcomes_hourly_local", "bytes_received"
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return []

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
