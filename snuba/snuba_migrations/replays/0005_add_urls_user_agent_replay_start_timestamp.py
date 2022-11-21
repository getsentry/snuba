from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, DateTime, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("urls", Array(String())), "url"),
    (Column("replay_start_timestamp", DateTime(Modifiers(nullable=True))), "timestamp"),
    # OS
    (Column("os_name", String(Modifiers(nullable=True))), "user_email"),
    (Column("os_version", String(Modifiers(nullable=True))), "os_name"),
    # Browser
    (Column("browser_name", String(Modifiers(nullable=True))), "os_version"),
    (Column("browser_version", String(Modifiers(nullable=True))), "browser_name"),
    # Device
    (Column("device_name", String(Modifiers(nullable=True))), "browser_version"),
    (Column("device_brand", String(Modifiers(nullable=True))), "device_name"),
    (Column("device_family", String(Modifiers(nullable=True))), "device_brand"),
    (Column("device_model", String(Modifiers(nullable=True))), "device_family"),
]


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]
        return new_column_ops

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_local", column.name)
            for column, _ in reversed(new_columns)
        ]

        return drop_column_ops

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]
        return new_column_ops

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_dist", column.name)
            for column, _ in reversed(new_columns)
        ]

        return drop_column_ops
