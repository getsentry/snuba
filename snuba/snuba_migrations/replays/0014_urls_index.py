from typing import Iterator, Sequence

from snuba.clickhouse.columns import Array, Column, UInt
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
    yield operations.AddColumn(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_local",
        column=Column(
            "_urls_hashed",
            Array(
                UInt(64),
                Modifiers(materialized="arrayMap(t -> cityHash64(t), urls)"),
            ),
        ),
        after="urls",
        target=operations.OperationTarget.LOCAL,
    )

    yield operations.AddColumn(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_dist",
        column=Column(
            "_urls_hashed",
            Array(
                UInt(64),
                Modifiers(materialized="arrayMap(t -> cityHash64(t), urls)"),
            ),
        ),
        after="urls",
        target=operations.OperationTarget.DISTRIBUTED,
    )

    yield operations.AddIndex(
        storage_set=StorageSetKey.REPLAYS,
        table_name="replays_local",
        index_name="bf_urls_hashed",
        index_expression="_urls_hashed",
        index_type="bloom_filter()",
        granularity=1,
    )


def backward_columns_iter() -> Iterator[operations.SqlOperation]:
    yield operations.DropColumn(
        StorageSetKey.REPLAYS,
        "replays_local",
        "_urls_hashed",
        operations.OperationTarget.LOCAL,
    )

    yield operations.DropColumn(
        StorageSetKey.REPLAYS,
        "replays_dist",
        "_urls_hashed",
        operations.OperationTarget.DISTRIBUTED,
    )

    yield operations.DropIndex(
        StorageSetKey.REPLAYS,
        "replays_local",
        "bf_urls_hashed",
    )
