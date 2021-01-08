from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.MultiStepMigration):
    """
    The transaction_name column should not be nullable; it is not in either errors or transacions
    """

    blocking = False

    def __forward_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column(
                    "transaction_name", String(Modifiers(low_cardinality=True))
                ),
            ),
        ]

    def __backwards_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column(
                    "transaction_name",
                    String(Modifiers(low_cardinality=True, nullable=True)),
                ),
            ),
        ]

    def forwards_local(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("discover_local")

    def backwards_local(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("discover_local")

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("discover_dist")

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("discover_dist")
