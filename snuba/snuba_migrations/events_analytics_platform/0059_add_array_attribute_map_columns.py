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
from snuba.utils.schemas import Array, Bool, Column, Float, Int, Map, String

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO
after = "attributes_float_39"

_codec = Modifiers(codecs=["ZSTD(1)"])

# Typed map columns for array-valued attributes, one column per element type.
# These replace the `attributes_array` JSON column (migration 0050) for storing
# array attributes: a `Map(String, Array(T))` lets us filter/aggregate on the
# values and enumerate keys with `mapKeys(...)` like the scalar attribute maps.
new_columns: list[Column[Modifiers]] = [
    Column("attributes_array_string", Map(String(), Array(String()), modifiers=_codec)),
    Column("attributes_array_int", Map(String(), Array(Int(64)), modifiers=_codec)),
    Column("attributes_array_float", Map(String(), Array(Float(64)), modifiers=_codec)),
    Column("attributes_array_bool", Map(String(), Array(Bool()), modifiers=_codec)),
]

# Downsample MVs are at mv_6 (migration 0058); bump 6 -> 7 so the regenerated
# views project the new columns from eap_items_1_local onto the downsample
# tables. The columns must already exist on both source and destination tables
# before the views are recreated, which is why AddColumn runs first below.
OLD_MV_VERSION = 6
NEW_MV_VERSION = 7

# get_eap_items_columns() returns a fresh list on each call; append locally so
# we do not mutate the shared helper (that would change already-applied
# migrations like 0058).
mv_columns = get_eap_items_columns()
mv_columns.extend(new_columns)


def _select(columns: list[Column[Modifiers]], sampling_weight: int) -> str:
    # Un-perturbed per-item hash, matching current master (migration 0058).
    return downsample_mv_select(
        columns,
        sampling_weight,
        where_predicate=f"cityHash64(item_id) % {sampling_weight}",
    )


def _tables() -> list[tuple[StorageSetKey, str, OperationTarget]]:
    tables: list[tuple[StorageSetKey, str, OperationTarget]] = [
        (storage_set, "eap_items_1_local", OperationTarget.LOCAL),
        (storage_set, "eap_items_1_dist", OperationTarget.DISTRIBUTED),
    ]
    for w in SAMPLING_WEIGHTS:
        tables.append((storage_set, f"eap_items_1_downsample_{w}_local", OperationTarget.LOCAL))
        tables.append(
            (storage_set, f"eap_items_1_downsample_{w}_dist", OperationTarget.DISTRIBUTED)
        )
    # The read-only distributed tables are created via `CREATE TABLE ... AS` and
    # do not inherit schema changes from their source, so add the columns
    # explicitly for the read path to see them.
    tables.append((ro_storage_set, "eap_items_1_dist_ro", OperationTarget.DISTRIBUTED))
    for w in SAMPLING_WEIGHTS:
        tables.append(
            (
                ro_storage_set,
                f"eap_items_1_downsample_{w}_dist_ro",
                OperationTarget.DISTRIBUTED,
            )
        )
    return tables


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        ops: list[SqlOperation] = [
            operations.AddColumn(
                storage_set=ss,
                table_name=table,
                column=col,
                after=after,
                target=target,
            )
            for (ss, table, target) in _tables()
            for col in new_columns
        ]
        # Columns now exist on the source and downsample tables; regenerate the
        # downsample views so they project the new columns.
        ops.extend(
            swap_downsample_materialized_views(
                columns=mv_columns,
                create_version=NEW_MV_VERSION,
                drop_version=OLD_MV_VERSION,
                query_for_weight=lambda w: _select(mv_columns, w),
            )
        )
        return ops

    def backwards_ops(self) -> list[SqlOperation]:
        # Restore the previous views (which do not reference the new columns)
        # before dropping the columns they would otherwise read from.
        ops: list[SqlOperation] = list(
            swap_downsample_materialized_views(
                columns=get_eap_items_columns(),
                create_version=OLD_MV_VERSION,
                drop_version=NEW_MV_VERSION,
                query_for_weight=lambda w: _select(get_eap_items_columns(), w),
            )
        )
        ops.extend(
            operations.DropColumn(
                storage_set=ss,
                table_name=table,
                column_name=col.name,
                target=target,
            )
            for (ss, table, target) in reversed(_tables())
            for col in new_columns
        )
        return ops
