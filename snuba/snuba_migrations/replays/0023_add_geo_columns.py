from typing import Iterator, Sequence, Tuple

from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations.columns import MigrationModifiers
from snuba.migrations.migration import ClickhouseNodeMigration
from snuba.migrations.operations import (
    AddColumn,
    DropColumn,
    OperationTarget,
    SqlOperation,
)
from snuba.utils.schemas import Column, String

new_columns: Sequence[Tuple[Column[MigrationModifiers], str]] = [
    (Column("user_geo_city", String()), "user_email"),
    (Column("user_geo_country_code", String()), "user_geo_city"),
    (Column("user_geo_region", String()), "user_geo_country_code"),
    (Column("user_geo_subdivision", String()), "user_geo_region"),
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
