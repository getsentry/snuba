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


def _tables() -> list[tuple[StorageSetKey, str]]:
    # The downsample materialized views never projected `attributes_array`
    # (they SELECT an explicit column list built from get_eap_items_columns(),
    # which has never included it), so the column only ever held default
    # values on the downsample tables and nothing references it anywhere.
    #
    # Only the distributed tables are touched here. Distributed tables hold no
    # data, so dropping the column is a cheap metadata-only change. The local
    # (MergeTree) tables deliberately keep the column: DROP COLUMN on a
    # MergeTree table rewrites every part even when the column is empty, which
    # is unacceptable in customer-facing environments. (A previous version of
    # this migration, 0062_drop_attributes_array_json_from_dist_and_downsample,
    # was reverted for exactly that reason.)
    tables: list[tuple[StorageSetKey, str]] = [(storage_set, "eap_items_1_dist")]
    for w in SAMPLING_WEIGHTS:
        tables.append((storage_set, f"eap_items_1_downsample_{w}_dist"))
    # The read-only distributed tables (created in 0056 via CREATE TABLE ... AS)
    # inherited the column from their source dist tables, so drop it there too.
    tables.append((ro_storage_set, "eap_items_1_dist_ro"))
    for w in SAMPLING_WEIGHTS:
        tables.append((ro_storage_set, f"eap_items_1_downsample_{w}_dist_ro"))
    return tables


class Migration(migration.ClickhouseNodeMigration):
    """Drop the `attributes_array` JSON column (added by migration 0050) from
    the distributed eap_items tables.

    The column was replaced by the typed attributes_array_{string,int,float,bool}
    map columns from migration 0059 and is no longer read. Local tables keep the
    column: dropping it from MergeTree tables would trigger a full part rewrite
    even though the column holds only default values. No materialized view
    projects this column, so no view regeneration is needed.
    """

    blocking = False

    def forwards_ops(self) -> list[SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=ss,
                table_name=table,
                column_name=column_name,
                target=OperationTarget.DISTRIBUTED,
            )
            for (ss, table) in _tables()
        ]

    def backwards_ops(self) -> list[SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=ss,
                table_name=table,
                column=attributes_array_column,
                after=after,
                target=OperationTarget.DISTRIBUTED,
            )
            for (ss, table) in reversed(_tables())
        ]
