from typing import Sequence


from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
    FixedString,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
    WithCodecs,
    WithDefault,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines

columns = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", WithCodecs(UUID(), ["NONE"])),
    Column(
        "event_hash",
        WithCodecs(
            Materialized(UInt(64), "cityHash64(toString(event_id))",), ["NONE"],
        ),
    ),
    Column("platform", LowCardinality(String())),
    Column("environment", LowCardinality(Nullable(String()))),
    Column("release", LowCardinality(Nullable(String()))),
    Column("dist", LowCardinality(Nullable(String()))),
    Column("ip_address_v4", Nullable(IPv4())),
    Column("ip_address_v6", Nullable(IPv6())),
    Column("user", WithDefault(String(), "''")),
    Column("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
    Column("user_id", Nullable(String())),
    Column("user_name", Nullable(String())),
    Column("user_email", Nullable(String())),
    Column("sdk_name", LowCardinality(Nullable(String()))),
    Column("sdk_version", LowCardinality(Nullable(String()))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("_contexts_flattened", String()),
    Column("transaction_name", WithDefault(LowCardinality(String()), "''")),
    Column("transaction_hash", Materialized(UInt(64), "cityHash64(transaction_name)"),),
    Column("span_id", Nullable(UInt(64))),
    Column("trace_id", Nullable(UUID())),
    Column("partition", UInt(16)),
    Column("offset", WithCodecs(UInt(64), ["DoubleDelta", "LZ4"])),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", FixedString(32)),
    Column("primary_hash_hex", Materialized(UInt(64), "hex(primary_hash)")),
    Column("event_string", WithCodecs(String(), ["NONE"])),
    Column("received", DateTime()),
    Column("message", String()),
    Column("title", String()),
    Column("culprit", String()),
    Column("level", LowCardinality(String())),
    Column("location", Nullable(String())),
    Column("version", LowCardinality(Nullable(String()))),
    Column("type", LowCardinality(String())),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", Nullable(String())),
                ("value", Nullable(String())),
                ("mechanism_type", Nullable(String())),
                ("mechanism_handled", Nullable(UInt(8))),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", Nullable(String())),
                ("colno", Nullable(UInt(32))),
                ("filename", Nullable(String())),
                ("function", Nullable(String())),
                ("lineno", Nullable(UInt(32))),
                ("in_app", Nullable(UInt(8))),
                ("package", Nullable(String())),
                ("module", Nullable(String())),
                ("stack_level", Nullable(UInt(16))),
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
