from typing import Iterator, Sequence, Tuple

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import (
    AddColumn,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.utils.schemas import Column, String

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (Column("tap_message", String()), "click_is_rage"),
    (Column("tap_view_class", String()), "tap_message"),
    (Column("tap_view_id", String()), "tap_view_class"),
]


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[SqlOperation]:
    for column, after in new_columns:
        yield AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=column,
            after=after,
            target=OperationTarget.LOCAL,
        )

        yield AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=column,
            after=after,
            target=OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[SqlOperation]:
    for column, _ in new_columns:
        yield DropColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            target=OperationTarget.DISTRIBUTED,
            column_name=column.name,
        )

        yield DropColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            target=OperationTarget.LOCAL,
            column_name=column.name,
        )
