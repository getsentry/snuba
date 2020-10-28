from typing import Sequence

from snuba.clickhouse.columns import (
    Column,
    DateTime,
    String,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.MultiStepMigration):
    """
    Add the materialized columns required for the Discover merge table.
    """

    blocking = False

    def __forward_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "type",
                    String(
                        Modifiers(low_cardinality=True, materialized="'transaction'")
                    ),
                ),
                after="deleted",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "message",
                    String(
                        Modifiers(low_cardinality=True, materialized="transaction_name")
                    ),
                ),
                after="type",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "title",
                    String(
                        Modifiers(low_cardinality=True, materialized="transaction_name")
                    ),
                ),
                after="message",
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name=table_name,
                column=Column(
                    "timestamp", DateTime(Modifiers(materialized="finish_ts"))
                ),
                after="title",
            ),
        ]

    def __backwards_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(StorageSetKey.TRANSACTIONS, table_name, "type"),
            operations.DropColumn(StorageSetKey.TRANSACTIONS, table_name, "message"),
            operations.DropColumn(StorageSetKey.TRANSACTIONS, table_name, "title"),
            operations.DropColumn(StorageSetKey.TRANSACTIONS, table_name, "timestamp"),
        ]

    def forwards_local(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("transactions_local")

    def backwards_local(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("transactions_local")

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("transactions_dist")

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("transactions_dist")
