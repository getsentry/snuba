from typing import List, Sequence

from snuba.clickhouse.columns import UUID, Column, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.migrations import migration, operations, table_engines
from snuba.migrations.columns import MigrationModifiers as Modifiers
from snuba.migrations.operations import OperationTarget, SqlOperation
from snuba.utils.schemas import DateTime64, Float, Int, Map

storage_set = StorageSetKey.EVENTS_ANALYTICS_PLATFORM
local_table_name = "ourlogs_2_local"
dist_table_name = "ourlogs_2_dist"

columns: List[Column[Modifiers]] = [
    Column("organization_id", UInt(64)),
    Column("project_id", UInt(64)),
    Column("trace_id", UUID()),  # optional
    Column("span_id", UInt(64)),  # optional
    Column("severity_text", String()),
    Column("severity_number", UInt(8)),
    Column("retention_days", UInt(16)),
    Column("timestamp", DateTime64(9)),  # nanosecond precision
    Column("body", String(modifiers=Modifiers(codecs=["ZSTD(1)"]))),
    Column("attr_string", Map(String(), String())),
    Column("attr_int", Map(String(), Int(64))),
    Column("attr_double", Map(String(), Float(64))),  # this is what OTEL calls it
    Column("attr_bool", Map(String(), UInt(8))),
]


class Migration(migration.ClickhouseNodeMigration):
    blocking = False

    def forwards_ops(self) -> Sequence[SqlOperation]:
        return [
            operations.CreateTable(
                storage_set=storage_set,
                table_name=local_table_name,
                columns=columns,
                engine=table_engines.MergeTree(
                    order_by="(organization_id, project_id, toDateTime(timestamp), trace_id)",
                    partition_by="(retention_days, toMonday(timestamp))",
                    settings={"index_granularity": "8192"},
                    storage_set=storage_set,
                    ttl="toDateTime(timestamp) + toIntervalDay(retention_days)",
                ),
                target=OperationTarget.LOCAL,
            ),
            operations.CreateTable(
                storage_set=storage_set,
                table_name=dist_table_name,
                columns=columns,
                engine=table_engines.Distributed(
                    local_table_name=local_table_name,
                    sharding_key="rand()",
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
