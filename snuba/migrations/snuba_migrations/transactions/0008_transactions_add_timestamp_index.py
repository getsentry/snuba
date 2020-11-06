from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="minmax_timestamp",
                index_expression="timestamp",
                index_type="minmax",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropIndex(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                index_name="minmax_timestamp",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return []

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return []
