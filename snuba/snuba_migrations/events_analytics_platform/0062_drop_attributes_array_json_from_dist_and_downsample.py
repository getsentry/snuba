from snuba.clickhouse.columns import JSON, Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.snuba_migrations.events_analytics_platform.templates import SAMPLING_WEIGHTS

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
ro_storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM_RO

column_name = "attributes_array"
after = "attributes_float_39"

# Column definition as added by migration 0050, needed to re-add the column
# on rollback.
attributes_array_column = Column(
    column_name,
    JSON(
        max_dynamic_paths=128,
        modifiers=Modifiers(codecs=["ZSTD(1)"]),
    ),
)


def _tables() -> list[tuple[StorageSetKey, str, OperationTarget]]:
    # The downsample materialized views never projected `attributes_array`
    # (they SELECT an explicit column list built from get_eap_items_columns(),
    # which has never included it), so the column only ever held default
    # values on the downsample tables and nothing references it anywhere.
    # It can therefore be dropped from the downsample tables entirely (local
    # and distributed); only the full-fidelity eap_items_1_local keeps it.
    tables: list[tuple[StorageSetKey, str, OperationTarget]] = [
        (storage_set, "eap_items_1_dist", OperationTarget.DISTRIBUTED),
    ]
    for w in SAMPLING_WEIGHTS:
        tables.append(
            (storage_set, f"eap_items_1_downsample_{w}_dist", OperationTarget.DISTRIBUTED)
        )
    # The read-only distributed tables (created in 0056 via CREATE TABLE ... AS)
    # inherited the column from their source dist tables, so drop it there too.
    tables.append((ro_storage_set, "eap_items_1_dist_ro", OperationTarget.DISTRIBUTED))
    for w in SAMPLING_WEIGHTS:
        tables.append(
            (ro_storage_set, f"eap_items_1_downsample_{w}_dist_ro", OperationTarget.DISTRIBUTED)
        )
    for w in SAMPLING_WEIGHTS:
        tables.append((storage_set, f"eap_items_1_downsample_{w}_local", OperationTarget.LOCAL))
    return tables


class Migration(migration.ClickhouseNodeMigration):
    """Drop the `attributes_array` JSON column (added by migration 0050) from
    the distributed eap_items tables and the downsampled tables entirely.

    The column was replaced by the typed attributes_array_{string,int,float,bool}
    map columns from migration 0059 and is no longer read. The downsample
    materialized views never projected it, so it holds only default values on
    the downsample tables; nothing references it there. The full-fidelity
    eap_items_1_local keeps the column, so no view regeneration is needed.
    """

    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=ss,
                table_name=table,
                column_name=column_name,
                target=target,
            )
            for (ss, table, target) in _tables()
        ]

    def backwards_ops(self) -> list[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=ss,
                table_name=table,
                column=attributes_array_column,
                after=after,
                target=target,
            )
            for (ss, table, target) in reversed(_tables())
        ]
