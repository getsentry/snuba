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

_codec = Modifiers(codecs=["ZSTD(1)"])

new_columns: List[Column[Modifiers]] = [
    Column("ai_conversation_id", String(modifiers=_codec)),
]
add_column_after = ["session_id"]

index_type = "bloom_filter"
index_granularity = 1
new_column_indexes = [(f"bf_{column.name}", column.name) for column in new_columns]

# mv_8 (migration 0060) -> mv_9: project ai_conversation_id, stop projecting
# conversation_id. The conversation_id column stays; its DROP COLUMN is a
# separate follow-up migration.
mv_old_version = 8
mv_new_version = mv_old_version + 1

# Columns the current mv_8 set carries that aren't in get_eap_items_columns():
# the array columns from 0059 and conversation_id/session_id (after trace_id)
# from 0060. Reconstructed here so get_eap_items_columns() stays untouched.
array_attribute_columns: List[Column[Modifiers]] = [
    Column("attributes_array_string", Map(String(), Array(String()), modifiers=_codec)),
    Column("attributes_array_int", Map(String(), Array(Int(64)), modifiers=_codec)),
    Column("attributes_array_float", Map(String(), Array(Float(64)), modifiers=_codec)),
    Column("attributes_array_bool", Map(String(), Array(Bool()), modifiers=_codec)),
]
conversation_id: Column[Modifiers] = Column("conversation_id", UUID())
session_id: Column[Modifiers] = Column("session_id", UUID())


def _mv8_columns() -> List[Column[Modifiers]]:
    columns = get_eap_items_columns()
    columns.extend(array_attribute_columns)
    at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:at] + [conversation_id, session_id] + columns[at:]


def _mv9_columns() -> List[Column[Modifiers]]:
    columns = get_eap_items_columns()
    columns.extend(array_attribute_columns)
    at = next(i for i, c in enumerate(columns) if c.name == "trace_id") + 1
    return columns[:at] + [session_id] + list(new_columns) + columns[at:]


def _query_for_weight(columns: List[Column[Modifiers]]) -> Callable[[int], str]:
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
    return [f"{prefix}_dist_ro" for prefix in _table_prefixes()]


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

        mv_columns = _mv9_columns()
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
        base_columns = _mv8_columns()
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
