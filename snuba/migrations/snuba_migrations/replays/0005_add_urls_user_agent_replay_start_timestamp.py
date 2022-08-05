from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, DateTime, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("urls", Array(String())), "url"),
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
        return new_column_ops

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=Column("url", String(Modifiers(nullable=True))),
                after="title",
            )
        ]

        return new_column_ops

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
        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                column=Column("url", String(Modifiers(nullable=True))),
                after="title",
            )
        ]

        return new_column_ops
