from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    UInt,
    Materialized,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.datasets.storages.events_common import TAGS_HASH_MAP_COLUMN


class Migration(migration.MultiStepMigration):
    """
    Adds the tags hash map column defined as Array(Int64) Materialized with
    arrayMap((k, v) -> cityHash64(
        concat(replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1'), '=', v)
    ), tags.key, tags.value

    This allows us to quickly find tag key-value pairs since we can
    add an index on this column.
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_local",
                column=Column(
                    "_tags_hash_map",
                    Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN),
                ),
                after="_tags_flattened",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS, "sentry_local", "_tags_hash_map"
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="sentry_dist",
                column=Column("_tags_hash_map", Array(UInt(64)),),
                after="_tags_flattened",
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropColumn(
                StorageSetKey.EVENTS, "sentry_local", "_tags_hash_map"
            )
        ]
