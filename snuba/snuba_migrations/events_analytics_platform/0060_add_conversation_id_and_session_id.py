from typing import Callable, List

from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.snuba_migrations.events_analytics_platform.templates import (
    SAMPLING_WEIGHTS,
    downsample_mv_select,
    get_eap_items_columns,
    swap_downsample_materialized_views,
)
from snuba.utils.schemas import UUID, Array, Bool, Float, Int, Map, String

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO
table_name_prefix = "eap_items_1"
sampling_weights = SAMPLING_WEIGHTS

# New UUID identifier columns, added after `trace_id`.
new_columns: List[Column[Modifiers]] = [
    Column("conversation_id", UUID()),
    Column("session_id", UUID()),
]
# Chained so the on-disk order is trace_id -> conversation_id -> session_id.
add_column_after = ["trace_id", "conversation_id"]

# Bloom-filter indexes (like `bf_indexed_name`) on the local MergeTree tables.
index_type = "bloom_filter"
index_granularity = 1
new_column_indexes = [(f"bf_{column.name}", column.name) for column in new_columns]

# master is at mv_7 (migration 0059); bump to mv_8 so the regenerated downsample
# views also project the new columns.
mv_old_version = 7
mv_new_version = mv_old_version + 1

# Array-attribute map columns added by migration 0059 (mv_7), appended after the
# last base column. Reconstructed here so the rebuilt views match the mv_7 column
# set without touching the shared get_eap_items_columns() helper.
_codec = Modifiers(codecs=["ZSTD(1)"])
array_attribute_columns: List[Column[Modifiers]] = [
    Column("attributes_array_string", Map(String(), Array(String()), modifiers=_codec)),
    Column("attributes_array_int", Map(String(), Array(Int(64)), modifiers=_codec)),
    Column("attributes_array_float", Map(String(), Array(Float(64)), modifiers=_codec)),
    Column("attributes_array_bool", Map(String(), Array(Bool()), modifiers=_codec)),
]


def _mv7_columns() -> List[Column[Modifiers]]:
    """The mv_7 column list: base columns plus the array columns from migration 0059."""
    columns = get_eap_items_columns()
    columns.extend(array_attribute_columns)
    return columns


def _mv_columns_with_new() -> List[Column[Modifiers]]:
    """The mv_7 column list with the new columns inserted after `trace_id`.

    Built locally rather than in ``get_eap_items_columns`` so the earlier
    migrations (0058, 0059) that call it are unaffected.
    """
    columns = _mv7_columns()
    insert_at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:insert_at] + list(new_columns) + columns[insert_at:]


def _query_for_weight(columns: List[Column[Modifiers]]) -> Callable[[int], str]:
    # Un-perturbed per-item hash, matching the current mv_7 sampling.
    def inner(sampling_weight: int) -> str:
        return downsample_mv_select(
            columns,
            sampling_weight,
            where_predicate=f"cityHash64(item_id) % {sampling_weight}",
        )

    return inner


def _table_prefixes() -> List[str]:
    return [table_name_prefix] + [
        f"eap_items_1_downsample_{sampling_weight}" for sampling_weight in sampling_weights
    ]


def _local_tables() -> List[str]:
    return [f"{prefix}_local" for prefix in _table_prefixes()]


def _ro_dist_tables() -> List[str]:
    # Read-only distributed tables (CREATE TABLE ... AS) don't inherit schema
    # changes from their source, so the columns are added explicitly.
    return [f"{table_name_prefix}_dist_ro"] + [
        f"eap_items_1_downsample_{sampling_weight}_dist_ro" for sampling_weight in sampling_weights
    ]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> List[SqlOperation]:
        ops: List[SqlOperation] = []

        for prefix in _table_prefixes():
            for column, after in zip(new_columns, add_column_after):
                ops.extend(
                    [
                        operations.AddColumn(
                            storage_set=storage_set,
                            table_name=f"{prefix}_local",
                            column=column,
                            after=after,
                            target=OperationTarget.LOCAL,
                        ),
                        operations.AddColumn(
                            storage_set=storage_set,
                            table_name=f"{prefix}_dist",
                            column=column,
                            after=after,
                            target=OperationTarget.DISTRIBUTED,
                        ),
                    ]
                )

        for ro_table in _ro_dist_tables():
            for column, after in zip(new_columns, add_column_after):
                ops.append(
                    operations.AddColumn(
                        storage_set=ro_storage_set,
                        table_name=ro_table,
                        column=column,
                        after=after,
                        target=OperationTarget.DISTRIBUTED,
                    )
                )

        for table_name in _local_tables():
            for index_name, index_expression in new_column_indexes:
                ops.append(
                    operations.AddIndex(
                        storage_set=storage_set,
                        table_name=table_name,
                        index_name=index_name,
                        index_expression=index_expression,
                        index_type=index_type,
                        granularity=index_granularity,
                        target=OperationTarget.LOCAL,
                    )
                )

        # Recreate the downsample views so they project the new columns (added above).
        mv_columns = _mv_columns_with_new()
        ops.extend(
            swap_downsample_materialized_views(
                columns=mv_columns,
                create_version=mv_new_version,
                drop_version=mv_old_version,
                query_for_weight=_query_for_weight(mv_columns),
            )
        )

        return ops

    def backwards_ops(self) -> List[SqlOperation]:
        # Restore the mv_7 views before dropping the columns they read from.
        base_columns = _mv7_columns()
        ops: List[SqlOperation] = list(
            swap_downsample_materialized_views(
                columns=base_columns,
                create_version=mv_old_version,
                drop_version=mv_new_version,
                query_for_weight=_query_for_weight(base_columns),
            )
        )

        for table_name in _local_tables():
            for index_name, _ in new_column_indexes:
                ops.append(
                    operations.DropIndex(
                        storage_set=storage_set,
                        table_name=table_name,
                        index_name=index_name,
                        target=OperationTarget.LOCAL,
                    )
                )

        for prefix in _table_prefixes():
            for column in reversed(new_columns):
                ops.extend(
                    [
                        operations.DropColumn(
                            storage_set=storage_set,
                            table_name=f"{prefix}_dist",
                            column_name=column.name,
                            target=OperationTarget.DISTRIBUTED,
                        ),
                        operations.DropColumn(
                            storage_set=storage_set,
                            table_name=f"{prefix}_local",
                            column_name=column.name,
                            target=OperationTarget.LOCAL,
                        ),
                    ]
                )

        for ro_table in _ro_dist_tables():
            for column in reversed(new_columns):
                ops.append(
                    operations.DropColumn(
                        storage_set=ro_storage_set,
                        table_name=ro_table,
                        column_name=column.name,
                        target=OperationTarget.DISTRIBUTED,
                    )
                )

        return ops
