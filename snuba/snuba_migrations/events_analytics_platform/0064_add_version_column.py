from snuba.clickhouse.columns import Column, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name_prefix = "eap_items_1"

new_column_name = "version"

# The local and distributed tables deliberately carry *different* DEFAULT
# expressions. This is not an oversight; see below.

# ADD COLUMN does not rewrite existing parts, so every row written before this
# migration has no `version` on disk and the local DEFAULT is what gets
# evaluated when those rows are read or merged. Once eap_items moves to
# ReplacingMergeTree(version), a non-deterministic local default such as
# now64() would hand those legacy rows a version of "whenever the merge
# happened", which is always newer than a genuine write -- so a stale legacy row
# would silently win over a real update. `0` is therefore the sentinel for
# "written before versioning existed", and it deterministically loses to
# everything else.
local_column: Column[Modifiers] = Column(
    new_column_name,
    UInt(
        64,
        modifiers=Modifiers(
            default="0",
            codecs=["ZSTD(1)"],
        ),
    ),
)

# Inserts arrive through the distributed table, and a Distributed table
# evaluates its own DEFAULT expressions on the initiator node and then ships the
# materialized value to the shard -- the local DEFAULT never fires for that
# path. (Declaring the column here *without* a default would not fall through to
# the local default either; it would ship an explicit 0.) So this is the default
# that actually assigns a version to live writes, until the consumer starts
# populating the column explicitly.
dist_column: Column[Modifiers] = Column(
    new_column_name,
    UInt(
        64,
        modifiers=Modifiers(
            default="toUnixTimestamp64Milli(now64(3))",
            codecs=["ZSTD(1)"],
        ),
    ),
)

# `version` is intentionally not added to eap_items_1_dist_ro. It is internal
# bookkeeping for the ReplacingMergeTree cutover and is absent from the storage
# YAML, so nothing the query layer generates can reference it. Migration 0056
# creates the _dist_ro tables before this migration runs, so fresh and existing
# deployments both end up without the column there -- no environment drift.
# If we ever want to read `version` through the read-only routing path, it
# should be added in its own migration.


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_local",
                column=local_column,
                target=OperationTarget.LOCAL,
            ),
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_dist",
                column=dist_column,
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_dist",
                column_name=new_column_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_local",
                column_name=new_column_name,
                target=OperationTarget.LOCAL,
            ),
        ]
