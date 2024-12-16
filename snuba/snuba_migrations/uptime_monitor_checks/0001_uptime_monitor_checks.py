from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation

storage_set = StorageSetKey.UPTIME_MONITOR_CHECKS
table_prefix = "uptime_monitor_checks"
local_table_name = f"{table_prefix}_local"
dist_table_name = f"{table_prefix}_dist"


# do i need any of the domain or mutable info in postgres?
columns: List[Column[Modifiers]] = [
    Column("project_id", UInt(64)),
    Column("environment_id", String(Modifiers(nullable=True))),
    Column("uptime_subscription_id", UInt(64)),
    Column("timestamp", DateTime()),
    Column("uptime_check_id", UUID()),
    Column("duration", UInt(32)),
    Column("location_id", UInt(32, Modifiers(nullable=True))),
    Column("status", UInt(16)),
    Column("timeout", UInt(16)),
    Column("trace_id", UUID()),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.ReplacingMergeTree(
                    # is this the right order by? what about tostartOfDay?
                    # should environment be in the order by?
                    order_by="(project_id, uptime_subscription_id, timestamp, uptime_check_id)",
                    partition_by="(toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set,
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="cityHash64(uptime_check_id)",
                ),
                target=OperationTarget.DISTRIBUTED,
            ),
        ]

    def backwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.DropTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                target=OperationTarget.DISTRIBUTED,
            ),
            operations.DropTable(
                storage_set=storage_set,
                table_name=local_table_name,
                target=OperationTarget.LOCAL,
            ),
        ]
