from typing import List, Sequence, Tuple

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("group_ids", Array(UInt(64))), "timestamp"),
    (
        Column(
            "_group_ids_hashed",
            Array(
                UInt(64),
                Modifiers(materialized="arrayMap(t -> cityHash64(t), group_ids)"),
            ),
        ),
        "group_ids",
    ),
]

new_indexes: List[operations.SqlOperation] = [
    operations.AddIndex(
        storage_set=StorageSetKey.TRANSACTIONS,
        table_name="transactions_local",
        index_name="bf_group_ids_hashed",
        index_expression="_group_ids_hashed",
        index_type="bloom_filter()",
        granularity=1,
    )
]

drop_indexes: List[operations.SqlOperation] = [
    operations.DropIndex(
        StorageSetKey.TRANSACTIONS, "transactions_local", "bf_group_ids_hashed"
    )
]


class Migration(migration.ClickhouseNodeMigration):
    """
    The group ids column is required to query for transaction events
    with the same performance issue.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        new_column_ops: List[operations.SqlOperation] = [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]
        return new_column_ops + new_indexes

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        drop_column_ops: List[operations.SqlOperation] = [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", column.name
            )
            for column, _ in reversed(new_columns)
        ]
        return drop_indexes + drop_column_ops

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=column,
                after=after,
            )
            for column, after in new_columns
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", column.name
            )
            for column, _ in reversed(new_columns)
        ]
