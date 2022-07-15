from typing import Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("urls", Array(String())), "title"),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_local", column.name)
            for column, _ in new_columns
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_dist", column.name)
            for column, _ in new_columns
        ]
