from snuba.clickhouse.columns import Column
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget
from snuba.utils.schemas import Int

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
table_name_prefix = "eap_items_1"

# Three integer timestamps sit on almost every item as entries of the
# `attributes_int` map: Relay writes `sentry.timestamp_precise`, and the consumer
# writes the two `sentry._internal.*` ones. A map stores the key string next to
# every value and has no codec that fits a timestamp, and `attributes_int` is one
# unbucketed column, so a read of any integer attribute pays for all three.
#
# Each timestamp gets a column of its own, named after its attribute. The DEFAULT
# reads the attribute, which makes the column correct from the moment it exists:
#
# * A part written before this migration has no such column on disk, so ClickHouse
#   evaluates the DEFAULT when it reads or merges the part.
# * An insert that leaves the column out, which is every insert until the consumer
#   writes the columns itself, stores the value of the DEFAULT.
#
# No backfill is needed, and readers can move to the columns before the writer
# does. An item without the attribute reads as 0, as the map lookup does.
#
# Until the consumer stops writing the map entries, new parts hold each timestamp
# twice. The storage gain comes with that follow-up, not with this migration.
#
# T64 crops the high bits a block of timestamps has in common, and compressed
# these columns better than Delta or DoubleDelta on trace metrics: rows are sorted
# by `timestamp` in seconds, so neighbouring values are close but not ordered.
# `retention_days` uses the same codec pair.
_ATTRIBUTE_BY_COLUMN = {
    # When the item happened, in nanoseconds.
    "timestamp_precise": "sentry.timestamp_precise",
    # When Sentry first received the item, in seconds. Not `received_at`: the
    # consumer reserves that name for the Kafka broker timestamp it can emit.
    "sentry_received_at": "sentry._internal.received_at",
    # When the consumer processed the item, in milliseconds.
    "ingested_at": "sentry._internal.ingested_at",
}

# The same DEFAULT on both tables: a Distributed table evaluates its own DEFAULT on
# the initiator and ships the result, so the local one never fires on that path.
columns: list[Column[Modifiers]] = [
    Column(
        column_name,
        Int(
            64,
            modifiers=Modifiers(
                default=f"attributes_int['{attribute_name}']",
                codecs=["T64", "ZSTD(1)"],
            ),
        ),
    )
    for column_name, attribute_name in _ATTRIBUTE_BY_COLUMN.items()
]

# The columns are not added to eap_items_1_dist_ro or to the downsample tiers.
# Nothing reads or writes them yet, and each of those tables gets a migration of
# its own, as `version` did in 0065-0067.


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.AddColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_{suffix}",
                column=column,
                target=target,
            )
            for suffix, target in (
                ("local", OperationTarget.LOCAL),
                ("dist", OperationTarget.DISTRIBUTED),
            )
            for column in columns
        ]

    def backwards_ops(self) -> list[operations.SqlOperation]:
        return [
            operations.DropColumn(
                storage_set=storage_set,
                table_name=f"{table_name_prefix}_{suffix}",
                column_name=column.name,
                target=target,
            )
            for suffix, target in (
                ("dist", OperationTarget.DISTRIBUTED),
                ("local", OperationTarget.LOCAL),
            )
            for column in columns
        ]
