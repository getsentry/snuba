from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_set_key import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=Column("url", String(Modifiers(nullable=True))),
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=Column("url", String(Modifiers(nullable=False, default="''"))),
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                column=Column("url", String(Modifiers(nullable=True))),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.ModifyColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                column=Column("url", String(Modifiers(nullable=False, default="''"))),
            )
        ]
