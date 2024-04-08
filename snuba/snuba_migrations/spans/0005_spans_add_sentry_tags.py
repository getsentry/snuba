from typing import Sequence

from snuba.clickhouse.columns import Array, Column, Nested, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import SENTRY_TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set_name = StorageSetKey.SPANS
local_table_name = "spans_local"
dist_table_name = "spans_dist"


class Migration(migration.ClickhouseNodeMigration):
    """
    Adds the sentry tags and a sentry tags hash map column defined as Array(Int64)
    Materialized with SENTRY_TAGS_HASH_MAP_COLUMN expression.
    This allows us to quickly find tag key-value pairs since we can
    add an index on this column.
    """

    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            # sentry_tags columns
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "sentry_tags", Nested([("key", String()), ("value", String())])
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "sentry_tags", Nested([("key", String()), ("value", String())])
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
            # sentry_tags_hash_map columns
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column=Column(
                    "_sentry_tags_hash_map",
                    Array(
                        UInt(64), Modifiers(materialized=SENTRY_TAGS_HASH_MAP_COLUMN)
                    ),
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column=Column(
                    "_sentry_tags_hash_map",
                    Array(
                        UInt(64), Modifiers(materialized=SENTRY_TAGS_HASH_MAP_COLUMN)
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
                column_name="_sentry_tags_hash_map",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="sentry_tags.key",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=dist_table_name,
                column_name="sentry_tags.value",
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="_sentry_tags_hash_map",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="sentry_tags.key",
                target=OperationTarget.LOCAL,
            ),
            operations.DropColumn(
                storage_set=storage_set_name,
                table_name=local_table_name,
                column_name="sentry_tags.value",
                target=OperationTarget.LOCAL,
            ),
        ]
