from typing import Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False
    forwards_local_first: bool = False
    backwards_local_first: bool = True

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                column_name="profile",
            )
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_local",
                column=Column("profile", String(Modifiers(codecs=["LZ4HC(9)"]))),
                after="received",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_dist",
                column_name="profile",
            )
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.PROFILES,
                table_name="profiles_dist",
                column=Column("profile", String(Modifiers(codecs=["LZ4HC(9)"]))),
                after="received",
            )
        ]
