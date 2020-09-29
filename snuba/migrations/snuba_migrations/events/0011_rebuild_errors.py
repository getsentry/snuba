from typing import Sequence

from snuba.clickhouse.columns import (
    Array,
    Column,
    DateTime,
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
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines


columns = [
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", WithCodecs(UUID(), ["NONE"])),
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
    Column("http_method", LowCardinality(Nullable(String()))),
    Column("http_referer", Nullable(String())),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("transaction_name", WithDefault(LowCardinality(String()), "''")),
    Column("transaction_hash", Materialized(UInt(64), "cityHash64(transaction_name)"),),
    Column("span_id", Nullable(UInt(64))),
    Column("trace_id", Nullable(UUID())),
    Column("partition", UInt(16)),
    Column("offset", WithCodecs(UInt(64), ["DoubleDelta", "LZ4"])),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("row_version", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", UUID()),
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

sample_expr = "cityHash64(toString(event_id))"


class Migration(migration.MultiStepMigration):
    """
    This migration rebuilds the errors table, with the following changes:

    - Dropped org_id column
    - Dropped event_string column
    - Dropped event_hash materialized column
    - Dropped primary_hash_hex materialized column
    - primary_hash type converted from FixedString32 to UUID
    - Replaced deleted with row_version
    - Primary key is now more in line with what we have for the events table:
        - no org_id
        - cityHash64(toString(event_id)) instead of primary_hash_hex, event_hash
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
                    version_column="row_version",
                    order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
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
                    Materialized(Array(UInt(64)), TAGS_HASH_MAP_COLUMN),
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
