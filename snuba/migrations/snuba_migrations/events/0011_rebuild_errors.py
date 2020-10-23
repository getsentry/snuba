from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import (
    LowCardinality,
    Materialized,
    WithCodecs,
    WithDefault,
)


columns = [
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", UUID(Modifiers(codecs=["NONE"]))),
    Column("platform", String(lowcardinality())),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("ip_address_v4", IPv4(nullable())),
    Column("ip_address_v6", IPv6(nullable())),
    Column("user", String([WithDefault("''")])),
    Column("user_hash", UInt(64, [Materialized("cityHash64(user)")])),
    Column("user_id", String(nullable())),
    Column("user_name", String(nullable())),
    Column("user_email", String(nullable())),
    Column("sdk_name", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("sdk_version", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_method", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_referer", String(nullable())),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("transaction_name", String([LowCardinality(), WithDefault("''")])),
    Column(
        "transaction_hash", UInt(64, [Materialized("cityHash64(transaction_name)")])
    ),
    Column("span_id", UInt(64, nullable())),
    Column("trace_id", UUID(nullable())),
    Column("partition", UInt(16)),
    Column("offset", UInt(64, [WithCodecs(["DoubleDelta", "LZ4"])])),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", UUID()),
    Column("received", DateTime()),
    Column("message", String()),
    Column("title", String()),
    Column("culprit", String()),
    Column("level", String(lowcardinality())),
    Column("location", String(nullable())),
    Column("version", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("type", String(lowcardinality())),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String(nullable())),
                ("value", String(nullable())),
                ("mechanism_type", String(nullable())),
                ("mechanism_handled", UInt(8, nullable())),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(nullable())),
                ("colno", UInt(32, nullable())),
                ("filename", String(nullable())),
                ("function", String(nullable())),
                ("lineno", UInt(32, nullable())),
                ("in_app", UInt(8, nullable())),
                ("package", String(nullable())),
                ("module", String(nullable())),
                ("stack_level", UInt(16, nullable())),
            ]
        ),
    ),
    Column("sdk_integrations", Array(String())),
    Column("modules", Nested([("name", String()), ("version", String())])),
]

sample_expr = "cityHash64(event_id)"


class Migration(migration.MultiStepMigration):
    """
    This migration rebuilds the errors table, with the following changes:

    - Dropped org_id column
    - Dropped event_string column
    - Dropped event_hash materialized column
    - Dropped primary_hash_hex materialized column
    - primary_hash type converted from FixedString32 to UUID
    - Replaced deleted with row_version
    - Primary key updated:
        - no org_id
        - primary_hash instead of primary_hash_hex
        - cityHash64(event_id) instead of event_hash
    - Sharding key for distributed table is also cityHash64(toString(event_id))
    - No _tags_flattened and _contexts_flattened since these are already unused
    - Partition key reflects retention_days value not only 30 or 90
    """

    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local_new",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.EVENTS,
                    version_column="deleted",
                    order_by="(project_id, toStartOfDay(timestamp), primary_hash, %s)"
                    % sample_expr,
                    partition_by="(retention_days, toMonday(timestamp))",
                    sample_by=sample_expr,
                    ttl="timestamp + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local_new",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), [Materialized(TAGS_HASH_MAP_COLUMN)]),
                ),
                after="tags",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_local",
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.EVENTS,
                old_table_name="errors_local_new",
                new_table_name="errors_local",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_local_new"
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist_new",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="errors_local", sharding_key=sample_expr,
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist_new",
                column=Column("_tags_hash_map", Array(UInt(64)),),
                after="tags",
            ),
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_dist",
            ),
            operations.RenameTable(
                storage_set=StorageSetKey.EVENTS,
                old_table_name="errors_dist_new",
                new_table_name="errors_dist",
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="errors_local", sharding_key=sample_expr,
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_dist_new"
            )
        ]
