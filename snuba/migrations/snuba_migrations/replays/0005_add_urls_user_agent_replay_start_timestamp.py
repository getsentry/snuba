from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import UUID, Array, Column, DateTime, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("urls", Array(UUID())), "url"),
    (Column("replay_start_timestamp", DateTime(Modifiers(nullable=True))), "timestamp"),
    (Column("user_agent", String(Modifiers(nullable=True))), "user"),
]


class Migration(migration.ClickhouseNodeMigration):
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

        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_dist", "url")
        ]

        return new_column_ops + drop_column_ops

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_local", column.name)
            for column, _ in reversed(new_columns)
        ]

        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=Column("url", String(Modifiers(nullable=True))),
                after="title",
            )
        ]

        return drop_column_ops + new_column_ops

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

        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_dist", "url")
        ]

        return new_column_ops + drop_column_ops

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_dist", column.name)
            for column, _ in reversed(new_columns)
        ]

        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=Column("url", String(Modifiers(nullable=True))),
                after="title",
            )
        ]

        return drop_column_ops + new_column_ops
