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
    Column("event_id", UUID()),
    Column("root_replay_id", UUID()),
    ### columns used by other sentry events
    Column("project_id", UInt(64)),
    # time columns
    Column("timestamp", DateTime()),
    Column("start_ts", DateTime()),
    Column("start_ms", UInt(16)),
    Column("finish_ts", DateTime(Modifiers(nullable=True))),
    Column("finish_ms", UInt(16, Modifiers(nullable=True))),
    Column("duration", UInt(32, Modifiers(nullable=True))),
    # release/environment info
    Column("platform", String()),
    Column("environment", String(Modifiers(nullable=True))),
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
    # deletion info
    Column("retention_days", UInt(16)),
    Column("deleted", UInt(8)),
    # title
    Column("title", String()),
    Column("associated_event_ids", Array(UUID()))
    # TODO: add ids of sub-events in nodestore / ids of filestore?
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.REPLAYS,
                version_column="deleted",
                table_name="replays_local",
                columns=raw_columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.REPLAYS,
                    order_by="(project_id, toStartOfDay(start_ts), cityHash64(event_id))",
                    partition_by="(retention_days, toMonday(received))",
                    settings={"index_granularity": "8192"},
                    ttl="received + toIntervalDay(retention_days)",
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.REPLAYS, table_name="replays_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.REPLAYS,
                table_name="replays_dist",
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name="replays_local", sharding_key="project_id",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.REPLAYS, table_name="replays_dist"
            ),
        ]
