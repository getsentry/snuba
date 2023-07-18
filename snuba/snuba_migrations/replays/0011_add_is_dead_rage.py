from typing import Iterator, Sequence, Tuple

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

# Columns to be added to the table.
new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("click_is_dead", UInt(8)), "click_title"),
    (Column("click_is_rage", UInt(8)), "click_is_dead"),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column, after in new_columns:
        yield operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=column,
            after=after,
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=column,
            after=after,
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column, _ in new_columns:
        yield operations.DropColumn(
            StorageSetKey.REPLAYS,
            "replays_dist",
            column.name,
            operations.OperationTarget.DISTRIBUTED,
        )

        yield operations.DropColumn(
            StorageSetKey.REPLAYS,
            "replays_local",
            column.name,
            operations.OperationTarget.LOCAL,
        )
