from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import AddIndicesData, OperationTarget, SqlOperation

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
    Column("_sort_timestamp", DateTime()),
    Column("duration", UInt(64)),
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

indices: Sequence[AddIndicesData] = [
    AddIndicesData(
        name="bf_trace_id",
        expression="trace_id",
        type="bloom_filter",
        granularity=1,
    )
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
                    # do i actually need primary key to be different than sorting key?
                    primary_key="(organization_id, project_id, _sort_timestamp, uptime_check_id)",
                    order_by="(organization_id, project_id, uptime_subscription_id, _sort_timestamp, uptime_check_id)",
                    partition_by="(retention_days, toMonday(_sort_timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set,
                    ttl="_sort_timestamp + toIntervalDay(retention_days)",
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
            operations.AddIndices(
                storage_set=storage_set,
                table_name=local_table_name,
                indices=indices,
                target=OperationTarget.LOCAL,
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
