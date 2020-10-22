from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    FixedString,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import (
    LowCardinality,
    Materialized,
    WithCodecs,
    WithDefault,
)

columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", (UUID([WithCodecs(["NONE"])]))),
    Column(
        "event_hash",
        UInt(
            64, [Materialized("cityHash64(toString(event_id))"), WithCodecs(["NONE"])]
        ),
    ),
    Column("platform", String([LowCardinality()])),
    Column("environment", String([Nullable(), LowCardinality()])),
    Column("release", String([Nullable(), LowCardinality()])),
    Column("dist", String([Nullable(), LowCardinality()])),
    Column("ip_address_v4", IPv4([Nullable()])),
    Column("ip_address_v6", IPv6([Nullable()])),
    Column("user", (String([WithDefault("''")]))),
    Column("user_hash", UInt(64, [Materialized("cityHash64(user)")])),
    Column("user_id", String([Nullable()])),
    Column("user_name", String([Nullable()])),
    Column("user_email", String([Nullable()])),
    Column("sdk_name", String([Nullable(), LowCardinality()])),
    Column("sdk_version", String([Nullable(), LowCardinality()])),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("_contexts_flattened", String()),
    Column("transaction_name", String([LowCardinality(), WithDefault("''")])),
    Column(
        "transaction_hash", UInt(64, [Materialized("cityHash64(transaction_name)")])
    ),
    Column("span_id", UInt(64, [Nullable()])),
    Column("trace_id", UUID([Nullable()])),
    Column("partition", UInt(16)),
    Column("offset", UInt(64, [WithCodecs(["DoubleDelta", "LZ4"])])),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", FixedString(32)),
    Column("primary_hash_hex", UInt(64, [Materialized("hex(primary_hash)")])),
    Column("event_string", String([WithCodecs(["NONE"])])),
    Column("received", DateTime()),
    Column("message", String()),
    Column("title", String()),
    Column("culprit", String()),
    Column("level", String([LowCardinality()])),
    Column("location", String([Nullable()])),
    Column("version", String([Nullable(), LowCardinality()])),
    Column("type", String([LowCardinality()])),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String([Nullable()])),
                ("value", String([Nullable()])),
                ("mechanism_type", String([Nullable()])),
                ("mechanism_handled", UInt(8, [Nullable()])),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String([Nullable()])),
                ("colno", UInt(32, [Nullable()])),
                ("filename", String([Nullable()])),
                ("function", String([Nullable()])),
                ("lineno", UInt(32, [Nullable()])),
                ("in_app", UInt(8, [Nullable()])),
                ("package", String([Nullable()])),
                ("module", String([Nullable()])),
                ("stack_level", UInt(16, [Nullable()])),
            ]
        ),
    ),
    Column("sdk_integrations", Array(String())),
    Column("modules", Nested([("name", String()), ("version", String())])),
]


class Migration(migration.MultiStepMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.EVENTS,
                    version_column="deleted",
                    order_by="(org_id, project_id, toStartOfDay(timestamp), primary_hash_hex, event_hash)",
                    partition_by="(toMonday(timestamp), if(retention_days = 30, 30, 90))",
                    sample_by="event_hash",
                    ttl="timestamp + toIntervalDay(retention_days)",
                    settings={"index_granularity": "8192"},
                ),
            )
        ]

    def backwards_local(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_local",
            )
        ]

    def forwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="errors_local", sharding_key="event_hash",
                ),
            )
        ]

    def backwards_dist(self) -> Sequence[operations.Operation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_dist"
            )
        ]
