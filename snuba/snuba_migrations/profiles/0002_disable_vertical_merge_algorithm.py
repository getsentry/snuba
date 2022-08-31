from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Use the horizontal merge algorithm
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.RunSql(
                storage_set=StorageSetKey.PROFILES,
                statement=(
                    "ALTER TABLE profiles_local MODIFY SETTING enable_vertical_merge_algorithm=0"
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.RunSql(
                storage_set=StorageSetKey.PROFILES,
                statement=(
                    "ALTER TABLE profiles_local MODIFY SETTING enable_vertical_merge_algorithm=1"
                ),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
