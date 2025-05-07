from typing import Iterator, Sequence, Tuple

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers

new_columns: Sequence[Tuple[Column[Modifiers], str]] = [
    (
        Column("ota_updates_channel", String(Modifiers(nullable=True))),
        "browser_version",
    ),
    (
        Column("ota_updates_runtime_version", String(Modifiers(nullable=True))),
        "ota_updates_channel",
    ),
    (
        Column("ota_updates_update_id", String(Modifiers(nullable=True))),
        "ota_updates_runtime_version",
    ),
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
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            target=operations.OperationTarget.DISTRIBUTED,
            column_name=column.name,
        )

        yield operations.DropColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            target=operations.OperationTarget.LOCAL,
            column_name=column.name,
        )
