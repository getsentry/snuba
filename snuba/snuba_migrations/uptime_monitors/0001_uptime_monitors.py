from typing import Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers

raw_columns: Sequence[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("environment_id", UInt(64, Modifiers(nullable=True))),
    Column("uptime_subscription_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("uptime_check_id", UUID()),
    Column("duration", UInt(32)),
    Column("location_id", UInt(32, Modifiers(nullable=True))),
    Column("status", UInt(16)),
    Column("timeout_at", UInt(16)),
    Column("trace_id", UUID()),
]


class Migration(migration.ClickhouseNodeMigrationLegacy):
    blocking = False

    def forwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.UPTIME_MONITORS,
                table_name="uptime_monitors_local",
                columns=raw_columns,
                engine=table_engines.ReplacingMergeTree(
                    storage_set=StorageSetKey.UPTIME_MONITORS,
                    order_by="(project_id, timestamp, uptime_subscription_id, uptime_check_id)",
                    partition_by="(toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                ),
            ),
        ]

    def backwards_local(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.UPTIME_MONITORS,
                table_name="uptime_monitors_local",
            ),
        ]

    def forwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=StorageSetKey.UPTIME_MONITORS,
                table_name="uptime_monitors_dist",
                columns=raw_columns,
                engine=table_engines.Distributed(
                    local_table_name="uptime_monitors_local",
                    sharding_key="cityHash64(uptime_check_id)",
                ),
            ),
        ]

    def backwards_dist(self) -> Sequence[operations.SqlOperation]:
        return [
            operations.DropTable(
                storage_set=StorageSetKey.UPTIME_MONITORS,
                table_name="uptime_monitors_dist",
            ),
        ]
