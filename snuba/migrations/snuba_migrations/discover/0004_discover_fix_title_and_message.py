from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    The title and message column should not be nullable, since they are not in errors
    or transactions.
    """

    blocking = False

    def __forward_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("title", String()),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("message", String()),
            ),
        ]

    def __backwards_migrations(
        self, table_name: str
    ) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("title", String(Modifiers(nullable=True))),
            ),
            operations.ModifyColumn(
                storage_set=StorageSetKey.DISCOVER,
                table_name=table_name,
                column=Column("message", String(Modifiers(nullable=True))),
            ),
        ]

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("discover_local")

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("discover_local")

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__forward_migrations("discover_dist")

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return self.__backwards_migrations("discover_dist")
