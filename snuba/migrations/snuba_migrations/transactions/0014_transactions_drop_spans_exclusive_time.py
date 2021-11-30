from typing import Sequence

from snuba.clickhouse.columns import Array, Column, Float
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigration):

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "spans.exclusive_time"
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("spans.exclusive_time", Array(Float(64))),
                after="spans.group",
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column("spans.exclusive_time", Array(Float(64))),
                ttl_month=("finish_ts", 1),
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "spans.exclusive_time"
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column("spans.exclusive_time", Array(Float(64))),
                after="spans.group",
            ),
        ]
