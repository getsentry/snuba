from typing import Sequence

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds back (again) the size, bytes_received columns to match schema in SaaS
    """

    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_local",
                Column("size", UInt(32)),
                target=operations.OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                StorageSetKey.OUTCOMES,
                "outcomes_raw_dist",
                Column("size", UInt(32)),
                target=operations.OperationTarget.DISTRIBUTED,
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
                "outcomes_raw_dist",
                "size",
                target=operations.OperationTarget.DISTRIBUTED,
            ),
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
