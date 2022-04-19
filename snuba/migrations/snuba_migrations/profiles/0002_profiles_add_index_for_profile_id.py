from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                index_name="bf_profiles_profile_id",
                index_expression="profile_id",
                index_type="bloom_filter()",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                index_name="bf_profiles_profile_id",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return []
