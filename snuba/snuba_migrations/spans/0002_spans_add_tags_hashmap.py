from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the tags hash map column defined as Array(Int64) Materialized with
    arrayMap((k, v) -> cityHash64(
        concat(replaceRegexpAll(k, '(\\=|\\\\)', '\\\\\\1'), '=', v)
    ), tags.key, tags.value

    This allows us to quickly find tag key-value pairs since we can
    add an index on this column.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="_tags_hash_map",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="_tags_hash_map",
                target=OperationTarget.LOCAL,
            ),
        ]
