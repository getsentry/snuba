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
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

raw_columns: Sequence[Column[Modifiers]] = [
    Column("replay_id", UUID()),
    Column("sequence_id", UInt(16)),
    Column("trace_ids", Array(UUID())),
    Column(
        "_trace_ids_hashed",
        UInt(64, Modifiers(materialized="arrayMap(t -> cityHash64(t), trace_ids)")),
    ),
    Column("title", String()),
    ### columns used by other sentry events
    Column("project_id", UInt(64)),
    # time columns
    Column("timestamp", DateTime()),
    # release/environment info
    Column("platform", String(Modifiers(low_cardinality=True))),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("release", String(Modifiers(nullable=True))),
    Column("dist", String(Modifiers(nullable=True))),
    Column("ip_address_v4", IPv4(Modifiers(nullable=True))),
    Column("ip_address_v6", IPv6(Modifiers(nullable=True))),
    # user columns
    Column("user", String()),
    Column("user_hash", UInt(64)),
    Column("user_id", String(Modifiers(nullable=True))),
    Column("user_name", String(Modifiers(nullable=True))),
    Column("user_email", String(Modifiers(nullable=True))),
    # sdk info
    Column("sdk_name", String()),
    Column("sdk_version", String()),
    Column("tags", Nested([("key", String()), ("value", String())])),
    # internal data
    Column("retention_days", UInt(16)),
    Column("partition", UInt(16)),
    Column("offset", UInt(64)),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                columns=raw_columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.REPLAYS,
                    order_by="(project_id, toStartOfDay(timestamp), cityHash64(replay_id), sequence_id)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
            ),
            operations.AddIndex(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
                index_name="bf_trace_ids_hashed",
                index_expression="_trace_ids_hashed",
                index_type="bloom_filter()",
                granularity=1,
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name="replays_local",
                    sharding_key="cityHash64(toString(replay_id))",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.REPLAYS, table_name="replays_dist"
            ),
        ]
