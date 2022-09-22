from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("is_archived", UInt(8, Modifiers(nullable=True))), "urls"),
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
