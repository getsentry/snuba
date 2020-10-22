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
    Column("event_id", UUID([WithCodecs(["NONE"])])),
    Column("platform", String([LowCardinality()])),
    Column("environment", String([Nullable(), LowCardinality()])),
    Column("release", String([Nullable(), LowCardinality()])),
    Column("dist", String([Nullable(), LowCardinality()])),
    Column("ip_address_v4", IPv4([Nullable()])),
    Column("ip_address_v6", IPv6([Nullable()])),
    Column("user", String([WithDefault("''")])),
    Column("user_hash", UInt(64, [Materialized("cityHash64(user)")])),
    Column("user_id", String([Nullable()])),
    Column("user_name", String([Nullable()])),
    Column("user_email", String([Nullable()])),
    Column("sdk_name", String([Nullable(), LowCardinality()])),
    Column("sdk_version", String([Nullable(), LowCardinality()])),
    Column("http_method", String([Nullable(), LowCardinality()])),
    Column("http_referer", String([Nullable()])),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("contexts", Nested([("key", String()), ("value", String())])),
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
    Column("primary_hash", UUID()),
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
