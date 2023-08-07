from typing import Iterator, List, Sequence

from snuba.clickhouse.columns import Column, DateTime, Float, UInt
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
    for column in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=column,
            target=operations.OperationTarget.LOCAL,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=column,
            target=operations.OperationTarget.DISTRIBUTED,
        )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    for column in columns:
        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=column,
            target=operations.OperationTarget.DISTRIBUTED,
        )

        yield operations.ModifyColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=column,
            target=operations.OperationTarget.LOCAL,
        )


columns: List[Column[Modifiers]] = [
    Column("click_is_dead", UInt(8, Modifiers(codecs=["Delta", "LZ4"]))),
    Column("click_is_rage", UInt(8, Modifiers(codecs=["Delta", "LZ4"]))),
    Column("error_sample_rate", Float(64, Modifiers(codecs=["Gorilla", "LZ4"]))),
    Column("is_archived", UInt(8, Modifiers(codecs=["Delta", "LZ4"]))),
    Column("offset", UInt(8, Modifiers(codecs=["DoubleDelta", "LZ4"]))),
    Column("project_id", UInt(64, Modifiers(codecs=["DoubleDelta", "LZ4"]))),
    Column("segment_id", UInt(64, Modifiers(codecs=["Delta", "LZ4"]))),
    Column("session_sample_rate", Float(64, Modifiers(codecs=["Gorilla", "LZ4"]))),
    Column("timestamp", DateTime(Modifiers(codecs=["DoubleDelta", "LZ4"]))),
]
