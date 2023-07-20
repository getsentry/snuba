from typing import Iterator, Sequence, Tuple

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Tuple[Column[Modifiers], Column[Modifiers]]] = [
    (
        Column("title", String(Modifiers(nullable=True))),
        Column("title", String(Modifiers(nullable=False, default="''"))),
    ),
    (
        Column("user", String(Modifiers(nullable=True))),
        Column("user", String(Modifiers(nullable=False, default="''"))),
    ),
    (
        Column("sdk_name", String(Modifiers(nullable=True))),
        Column("sdk_name", String(Modifiers(nullable=False, default="''"))),
    ),
    (
        Column("sdk_version", String(Modifiers(nullable=True))),
        Column("sdk_version", String(Modifiers(nullable=False, default="''"))),
    ),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.SqlOperation]:
    for up_column, _ in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=up_column,
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=up_column,
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for _, down_column in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=down_column,
            target=operations.OperationTarget.DISTRIBUTED,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=down_column,
            target=operations.OperationTarget.LOCAL,
        )
