from typing import Iterator, Sequence

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import (
    AddColumn,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.utils.schemas import Array, Column, String

column: Column[Modifiers] = Column("segment_names", Array(String()))
after = "urls"


class Migration(ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return list(forward_iter())

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return list(backward_iter())


def forward_iter() -> Iterator[SqlOperation]:
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


def backward_iter() -> Iterator[SqlOperation]:
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
