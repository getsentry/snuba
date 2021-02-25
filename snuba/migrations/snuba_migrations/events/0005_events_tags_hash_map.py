from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the tags hash map column defined as Array(Int64) Materialized with
    arrayMap((k, v) -> cityHash64(
        concat(replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1'), '=', v)
    ), tags.key, tags.value

    This allows us to quickly find tag key-value pairs since we can
    add an index on this column.
    """

    blocking = True

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="_tags_flattened",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS, "sentry_local", "_tags_hash_map"
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_dist",
                column=Column("_tags_hash_map", Array(UInt(64)),),
                after="_tags_flattened",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropColumn(StorageSetKey.EVENTS, "sentry_dist", "_tags_hash_map")
        ]
