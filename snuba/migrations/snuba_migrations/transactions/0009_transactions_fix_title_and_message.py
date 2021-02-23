from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Removes the low cardinality modifier on the title and message fields. These column
    types need to match the corresponding fields in the errors table for Discover.
    """

    blocking = False

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "title", String(Modifiers(materialized="transaction_name")),
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "message", String(Modifiers(materialized="transaction_name")),
                ),
            ),
        ]

    def __backward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "title",
                    String(
                        Modifiers(low_cardinality=True, materialized="transaction_name")
                    ),
                ),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "message",
                    String(
                        Modifiers(low_cardinality=True, materialized="transaction_name")
                    ),
                ),
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("transactions_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("transactions_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backward_migrations("transactions_dist")
