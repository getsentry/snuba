from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[operations.SqlOperation]:
        return forward_columns_iter()

    def backwards_ops(self) -> Sequence[operations.SqlOperation]:
        return backward_columns_iter()


def forward_columns_iter() -> list[operations.SqlOperation]:
    return [
        operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            column=Column(
                "_tags_hash_map",
                Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
            ),
            after="tags.value",
            target=operations.OperationTarget.LOCAL,
        ),
        operations.AddColumn(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_dist",
            column=Column(
                "_tags_hash_map",
                Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
            ),
            after="tags.value",
            target=operations.OperationTarget.DISTRIBUTED,
        ),
        operations.AddIndex(
            storage_set=StorageSetKey.REPLAYS,
            table_name="replays_local",
            index_name="bf_tags_hash_map",
            index_expression="_tags_hash_map",
            index_type="bloom_filter",
            granularity=1,
            target=operations.OperationTarget.LOCAL,
        ),
    ]


def backward_columns_iter() -> list[operations.SqlOperation]:
    return [
        operations.DropIndex(
            StorageSetKey.REPLAYS,
            "replays_local",
            "bf_tags_hash_map",
            target=operations.OperationTarget.LOCAL,
        ),
        operations.DropColumn(
            StorageSetKey.REPLAYS,
            "replays_dist",
            "_tags_hash_map",
            operations.OperationTarget.DISTRIBUTED,
        ),
        operations.DropColumn(
            StorageSetKey.REPLAYS,
            "replays_local",
            "_tags_hash_map",
            operations.OperationTarget.LOCAL,
        ),
    ]
