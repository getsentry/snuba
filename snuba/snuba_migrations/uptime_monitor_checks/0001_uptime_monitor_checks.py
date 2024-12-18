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

## what about all the fancy codecs? do we need those?
columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("environment", String(Modifiers(nullable=True, low_cardinality=True))),
    Column("uptime_subscription_id", UInt(64)),
    Column("uptime_check_id", UUID()),
    Column("scheduled_check_time", DateTime()),
    Column("timestamp", DateTime()),
    Column("duration_ms", UInt(64)),
    Column("region_id", UInt(16, Modifiers(nullable=True))),
    Column("check_status", String(Modifiers(low_cardinality=True))),
    Column(
        "check_status_reason",
        String(Modifiers(nullable=True, low_cardinality=True)),
    ),
    Column("http_status_code", UInt(16)),
    Column("trace_id", UUID()),
    Column("retention_days", UInt(16)),
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
                    primary_key="(organization_id, project_id, timestamp, uptime_check_id, trace_id)",
                    order_by="(organization_id, project_id, timestamp, uptime_check_id, trace_id)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set,
                    ttl="timestamp + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="cityHash64(reinterpretAsUInt128(trace_id))",
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
