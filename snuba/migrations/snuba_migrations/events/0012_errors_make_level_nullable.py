from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.MultiStepMigration):
    blocking = False

    def __forward_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column=Column("level", String(Modifiers(nullable=True))),
            )
        ]

    def __backwards_migrations(self, table_name: str) -> Sequence[operations.Operation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name=table_name,
                column=Column("level", String()),
            )
        ]

    def forwards_local(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("errors_local")

    def backwards_local(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("errors_local")

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return self.__forward_migrations("errors_dist")

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return self.__backwards_migrations("errors_dist")
