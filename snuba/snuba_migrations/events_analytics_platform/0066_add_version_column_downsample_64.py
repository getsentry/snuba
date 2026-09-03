from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "eap_items_1_downsample_64_local"

new_column_name = "version"

# Mirrors the `version` column added to eap_items_1 in migration 0064, so this
# downsample tier's schema lines up with its target ReplacingMergeTree before
# existing parts are attached onto it.
#
# ADD COLUMN does not rewrite existing parts, so rows written before this
# migration have no `version` on disk and this DEFAULT is what gets evaluated
# when they are read or merged. It therefore has to be deterministic: under
# ReplacingMergeTree(version) a now64() default would hand those legacy rows a
# version of "whenever the merge happened", which is always newer than a real
# write, letting a stale row silently beat a genuine update. `0` is the sentinel
# for "written before versioning existed" and deterministically loses to
# everything else.
new_column: Column[Modifiers] = Column(
    new_column_name,
    UInt(
        64,
        modifiers=Modifiers(
            default="0",
            codecs=["ZSTD(1)"],
        ),
    ),
)

# Only the *_local table gets the column, unlike 0064 which also touched
# eap_items_1_dist. `version` is internal bookkeeping for the
# ReplacingMergeTree cutover: it is absent from the storage YAML so we never
# query it, and the downsample tiers are fed by materialized views that write
# straight to *_local, so the distributed table is never an insert target
# either. A Distributed table that declares a subset of its local table's
# columns reads normally; only a query naming `version` through the dist table
# would fail, and nothing does that. The same reasoning covers *_dist_ro.


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=local_table_name,
                column=new_column,
                target=OperationTarget.LOCAL,
            ),
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=local_table_name,
                column_name=new_column_name,
                target=OperationTarget.LOCAL,
            ),
        ]
