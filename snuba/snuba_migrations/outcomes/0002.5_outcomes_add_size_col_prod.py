from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    Updates materialized view query to support category and quantity.
    Note that the consumer needs to be stopped for this migration.
    """

    blocking = True

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                Column("size", UInt(64)),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_hourly_local",
                Column("bytes_received", UInt(64)),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_hourly_dist",
                Column("bytes_received", UInt(64)),
                target=operations.OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                "size",
                target=operations.OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_hourly_dist",
                "size",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_hourly_local",
                "size",
                target=operations.OperationTarget.LOCAL,
            ),
        ]
