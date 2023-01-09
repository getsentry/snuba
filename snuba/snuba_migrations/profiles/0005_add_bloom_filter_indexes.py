from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                index_name="bf_profiles_profile_id",
                index_expression="profile_id",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                index_name="bf_profiles_transaction_id",
                index_expression="transaction_id",
                index_type="bloom_filter",
                granularity=1,
                target=operations.OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropIndex(
                StorageSetKey.PROFILES,
                "profiles_local",
                "bf_profiles_profile_id",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropIndex(
                StorageSetKey.PROFILES,
                "profiles_local",
                "bf_profiles_transaction_id",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
