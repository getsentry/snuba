from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    The group ids column is required to query for transaction events
    with the same performance issue.
    """

    blocking = False

    def __forward_migrations(self, table_name: str) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column("group_ids", Array(UInt(64))),
                after="timestamp",
            )
        ]

    def __backwards_migrations(self, table_name: str) -> Sequence[operations.SqlOperation]:
        return [operations.DropColumn(StorageSetKey.TRANSACTIONS, table_name, "group_ids")]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("transactions_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("transactions_dist")
