from typing import Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.operations import OperationTarget


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyTableTTL(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_raw_local",
                reference_column="timestamp",
                ttl_days=30,
                target=OperationTarget.LOCAL,
            ),
            operations.ModifyTableTTL(
                storage_set=StorageSetKey.OUTCOMES,
                table_name="outcomes_hourly_local",
                reference_column="timestamp",
                ttl_days=90,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        """
        Removing a TTL is not supported by Clickhouse version 20.3. So there is
        no backwards migration.
        """
        return []
