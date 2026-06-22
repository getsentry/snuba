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
from snuba.utils.schemas import UUID

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO
table_name_prefix = "eap_items_1"
sampling_weights = SAMPLING_WEIGHTS

# Two new identifier columns, stored as UUID to match the existing `trace_id`
# column. They are added right after `trace_id` so the on-disk order stays
# stable: trace_id, conversation_id, session_id.
new_columns: List[Column[Modifiers]] = [
    Column("conversation_id", UUID()),
    Column("session_id", UUID()),
]
# `after` targets chained so the final order is
# trace_id -> conversation_id -> session_id.
add_column_after = ["trace_id", "conversation_id"]

# Bloom-filter indexes on the new columns (mirrors `bf_indexed_name` from
# migration 0057) so equality lookups can skip granules. Indexes live on the
# MergeTree local tables only.
index_type = "bloom_filter"
index_granularity = 1
new_column_indexes = [(f"bf_{column.name}", column.name) for column in new_columns]

# master is at mv_6 (migration 0058_nest_downsample_tiers), so this migration
# bumps 6 -> 7. The downsample materialized views select an explicit column list
# from eap_items_1_local, so the new columns have to be added to the views (not
# just the tables) for them to be populated on the downsampled read paths.
mv_old_version = 6
mv_new_version = mv_old_version + 1


def _mv_columns_with_new() -> List[Column[Modifiers]]:
    """The mv_6 column list with the new columns inserted after `trace_id`.

    Built locally instead of in ``get_eap_items_columns`` because migration 0058
    runs before this one and also calls ``get_eap_items_columns``; adding the new
    columns there would make 0058 build its views referencing columns that do not
    exist yet when it runs.
    """
    columns = get_eap_items_columns()
    insert_at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:insert_at] + list(new_columns) + columns[insert_at:]


def _query_for_weight(columns: List[Column[Modifiers]]) -> Callable[[int], str]:
    # Predicate matches the current (mv_6) nested-subset sampling introduced by
    # migration 0058: a single, un-perturbed hash on item_id.
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
    # The read-only distributed tables (created via `CREATE TABLE ... AS`) do not
    # inherit schema changes from their source tables, so the columns must be
    # added explicitly for queries on the read path to see them.
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

        # Recreate the downsample materialized views so they select the new
        # columns. The columns must already exist on eap_items_1_local and the
        # downsampled tables (added above) before the views are recreated.
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
        # Restore the previous materialized views (which do not reference the new
        # columns) before dropping the columns the new views read from.
        base_columns = get_eap_items_columns()
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
