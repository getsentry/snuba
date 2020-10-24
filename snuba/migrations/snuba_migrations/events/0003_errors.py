from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    Column,
    DateTime,
    FixedString,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.columns import lowcardinality, nullable

columns: Sequence[Column[Modifiers]] = [
    Column("org_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", (UUID(Modifiers(codecs=["NONE"])))),
    Column(
        "event_hash",
        UInt(
            64,
            Modifiers(materialized="cityHash64(toString(event_id))", codecs=["NONE"]),
        ),
    ),
    Column("platform", String(lowcardinality())),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("ip_address_v4", IPv4(nullable())),
    Column("ip_address_v6", IPv6(nullable())),
    Column("user", (String(Modifiers(default="''")))),
    Column("user_hash", UInt(64, Modifiers(materialized="cityHash64(user)"))),
    Column("user_id", String(nullable())),
    Column("user_name", String(nullable())),
    Column("user_email", String(nullable())),
    Column("sdk_name", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("sdk_version", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("_tags_flattened", String()),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("_contexts_flattened", String()),
    Column("transaction_name", String(Modifiers(low_cardinality=True, default="''"))),
    Column(
        "transaction_hash",
        UInt(64, Modifiers(materialized="cityHash64(transaction_name)")),
    ),
    Column("span_id", UInt(64, nullable())),
    Column("trace_id", UUID(nullable())),
    Column("partition", UInt(16)),
    Column("offset", UInt(64, Modifiers(codecs=["DoubleDelta", "LZ4"]))),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", FixedString(32)),
    Column("primary_hash_hex", UInt(64, Modifiers(materialized="hex(primary_hash)"))),
    Column("event_string", String(Modifiers(codecs=["NONE"]))),
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
