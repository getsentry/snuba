from typing import Iterator, List, Sequence

from snuba.clickhouse.columns import Column, String
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(forward_columns_iter())

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return list(backward_columns_iter())


def forward_columns_iter() -> Iterator[operations.ModifyColumn]:
    for column_name in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(
                column_name, String(Modifiers(nullable=True, low_cardinality=True))
            ),
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(
                column_name, String(Modifiers(nullable=True, low_cardinality=True))
            ),
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column_name in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(
                column_name, String(Modifiers(nullable=True, low_cardinality=False))
            ),
            target=operations.OperationTarget.DISTRIBUTED,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(
                column_name, String(Modifiers(nullable=True, low_cardinality=False))
            ),
            target=operations.OperationTarget.LOCAL,
        )


columns: List[str] = [
    "browser_name",
    "device_brand",
    "device_family",
    "device_model",
    "device_name",
    "os_name",
    "sdk_name",
    "sdk_version",
]
