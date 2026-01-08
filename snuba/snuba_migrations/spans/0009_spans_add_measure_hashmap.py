from typing import Sequence

from snuba.clickhouse.columns import Array, Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"

# There an issue in Clickhouse where the arrayMap function passes
# in Nothing type values for empty arrays. This causes the regex function to fail
# without the toString function, unless a merge for the part is completed.
# This is fixed in Clickhouse 22.
MEASUREMENTS_HASH_MAP_COLUMN = (
    "arrayMap((k) -> "
    "cityHash64(replaceRegexpAll(toString(k), '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1')),"
    "measurements.key)"
)


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds measurements tags hash map column defined as Array(Int64)
    Materialized with MEASUREMENTS_HASH_MAP_COLUMN expression.
    This allows us to quickly find measure tag keys and we can add an
    index on this column. Unlike sentry tags, we don't hash the
    tag values since we don't need to query on them.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "_measurements_hash_map",
                    Array(
                        UInt(64), Modifiers(materialized=MEASUREMENTS_HASH_MAP_COLUMN)
                    ),
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "_measurements_hash_map",
                    Array(
                        UInt(64), Modifiers(materialized=MEASUREMENTS_HASH_MAP_COLUMN)
                    ),
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="_measurements_hash_map",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="_measurements_hash_map",
                target=OperationTarget.LOCAL,
            ),
        ]
