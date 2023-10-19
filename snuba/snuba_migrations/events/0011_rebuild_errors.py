from typing import Sequence

from snuba.clickhouse.columns import (
    UUID,
    Array,
    Column,
    DateTime,
    IPv4,
    IPv6,
    Nested,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storages.tags_hash_map import TAGS_HASH_MAP_COLUMN
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

columns: Sequence[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("event_id", UUID(Modifiers(codecs=["NONE"]))),
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("dist", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    Column("user", String(Modifiers(default="''"))),
    Column("user_hash", UInt(64, Modifiers(materialized="cityHash64(user)"))),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    Column("sdk_name", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("sdk_version", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_method", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("http_referer", String(Modifiers(nullable=True))),
    Column("tags", Nested([("key", String()), ("value", String())])),
    Column("contexts", Nested([("key", String()), ("value", String())])),
    Column("transaction_name", String(Modifiers(low_cardinality=True, default="''"))),
    Column(
        "transaction_hash",
        UInt(64, Modifiers(materialized="cityHash64(transaction_name)")),
    ),
    Column("span_id", UInt(64, Modifiers(nullable=True))),
    Column("trace_id", UUID(Modifiers(nullable=True))),
    Column("partition", UInt(16)),
    Column("offset", UInt(64, Modifiers(codecs=["DoubleDelta", "LZ4"]))),
    Column("message_timestamp", DateTime()),
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    Column("group_id", UInt(64)),
    Column("primary_hash", UUID()),
    Column("received", DateTime()),
    Column("message", String()),
    Column("title", String()),
    Column("culprit", String()),
    Column("level", String(Modifiers(low_cardinality=True))),
    Column("location", String(Modifiers(nullable=True))),
    Column("version", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("type", String(Modifiers(low_cardinality=True))),
    Column(
        "exception_stacks",
        Nested(
            [
                ("type", String(Modifiers(nullable=True))),
                ("value", String(Modifiers(nullable=True))),
                ("mechanism_type", String(Modifiers(nullable=True))),
                ("mechanism_handled", UInt(8, Modifiers(nullable=True))),
            ]
        ),
    ),
    Column(
        "exception_frames",
        Nested(
            [
                ("abs_path", String(Modifiers(nullable=True))),
                ("colno", UInt(32, Modifiers(nullable=True))),
                ("filename", String(Modifiers(nullable=True))),
                ("function", String(Modifiers(nullable=True))),
                ("lineno", UInt(32, Modifiers(nullable=True))),
                ("in_app", UInt(8, Modifiers(nullable=True))),
                ("package", String(Modifiers(nullable=True))),
                ("module", String(Modifiers(nullable=True))),
                ("stack_level", UInt(16, Modifiers(nullable=True))),
            ]
        ),
    ),
    Column("sdk_integrations", Array(String())),
    Column("modules", Nested([("name", String()), ("version", String())])),
]

sample_expr = "cityHash64(event_id)"


class Migration(migration.ClickhouseNodeMigrationLegacy):
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

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_local",
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
                table_name="errors_local",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags",
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_local"
            )
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="errors_local",
                    sharding_key=sample_expr,
                ),
            ),
            operations.AddColumn(
                storage_set=StorageSetKey.EVENTS,
                table_name="errors_dist",
                column=Column(
                    "_tags_hash_map",
                    Array(UInt(64), Modifiers(materialized=TAGS_HASH_MAP_COLUMN)),
                ),
                after="tags",
            ),
            operations.CreateTable(
                storage_set=StorageSetKey.EVENTS_RO,
                table_name="errors_dist_ro",
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name="errors_local",
                    sharding_key=sample_expr,
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS, table_name="errors_dist"
            ),
            operations.DropTable(
                storage_set=StorageSetKey.EVENTS_RO, table_name="errors_dist_ro"
            ),
        ]
