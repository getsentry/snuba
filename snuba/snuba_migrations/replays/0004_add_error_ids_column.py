from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import UUID, Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("error_ids", Array(UUID())), "url"),
    (
        Column(
            "_error_ids_hashed",
            Array(
                UInt(64),
                Modifiers(materialized="arrayMap(t -> cityHash64(t), error_ids)"),
            ),
        ),
        "error_ids",
    ),
]

new_indexes: List[operations.SqlOperation] = [
    operations.AddIndex(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_local",
        index_name="bf_error_ids_hashed",
        index_expression="_error_ids_hashed",
        index_type="bloom_filter()",
        granularity=1,
    ),
]

drop_indexes: List[operations.SqlOperation] = [
    operations.DropIndex(
        StorageSetKey.REPLAYS,
        "replays_local",
        "bf_error_ids_hashed",
    )
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
        return new_column_ops + new_indexes

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(StorageSetKey.REPLAYS, "replays_local", column.name)
            for column, _ in reversed(new_columns)
        ]
        return drop_indexes + drop_column_ops

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
            for column, _ in reversed(new_columns)
        ]
