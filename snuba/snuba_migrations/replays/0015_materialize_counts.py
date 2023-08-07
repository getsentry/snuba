from typing import Iterator, List, Sequence, Tuple

from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.SqlOperation]:
    for after, column in columns:
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
    for _, column in columns:
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


columns: List[Tuple[str, Column[Modifiers]]] = [
    (
        "click_is_rage",
        Column("count_errors", UInt(16, Modifiers(materialized="length(error_ids)"))),
    ),
    (
        "count_errors",
        Column("count_urls", UInt(16, Modifiers(materialized="length(urls)"))),
    ),
]
