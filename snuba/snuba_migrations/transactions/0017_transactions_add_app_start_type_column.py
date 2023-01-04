from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigrationLegacy):
    """
    The app start type column is required to query for different
    app start types.
    """

    blocking = False

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "app_start_type",
                    String(Modifiers(low_cardinality=True)),
                ),
                after="group_ids",
            ),
        ]

    def __backwards_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, table_name, "app_start_type"
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("transactions_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("transactions_dist")
