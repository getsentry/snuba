from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import Materialized


class Migration(migration.MultiStepMigration):
    """
    Adds the tags hash map column defined as Array(Int64) Materialized with
    arrayMap((k, v) -> cityHash64(
        concat(replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1'), '=', v)
    ), tags.key, tags.value

    This allows us to quickly find tag key-value pairs since we can
    add an index on this column.
    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_local",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), [Materialized(TAGS_HASH_MAP_COLUMN)]),
                ),
                after="_tags_flattened",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_local", "_tags_hash_map"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.TRANSACTIONS,
                table_name="transactions_dist",
                column=Column("_tags_hash_map", Array(UInt(64)),),
                after="_tags_flattened",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.TRANSACTIONS, "transactions_dist", "_tags_hash_map"
            )
        ]
